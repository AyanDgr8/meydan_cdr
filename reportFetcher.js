// reportFetcher.js
// Generic report fetcher for call-center portal tables.
// Supports the following endpoints:
//   â€“ /api/v2/reports/cdrs                         (CDRs)
//   â€“ /api/v2/reports/queues_cdrs                  (Queue Calls)
//   â€“ /api/v2/reports/queues_outbound_cdrs         (Queue Outbound Calls)
//   â€“ /api/v2/reports/campaigns/leads/history      (Campaigns Activity)
//
// This module handles:
//   â€¢ Portal authentication via tokenService.getPortalToken
//   â€¢ Automatic pagination via next_start_key when provided
//   â€¢ Exponential-backoff retry logic (up to 3 attempts)
//   â€¢ Data manipulation and conversion for frontend display
//   â€¢ Optional CSV serialization helper
//   â€¢ A minimal CLI for ad-hoc usage

import axios from 'axios';
import fs from 'fs';
import path from 'path';
import { getPortalToken, httpsAgent } from './tokenService.js';
import { parsePhoneNumberFromString, getCountryCallingCode } from 'libphonenumber-js';
import ms from 'ms';
import dbService from './dbService.js';
import { isQueueExtension, findLastHoldStartForQueueExtension, findMatchingInboundCallByHoldTime } from './transferQueueDetector.js';
import { fileURLToPath } from 'url';

// Removed getTransferredCalls import as it's no longer needed
// Removed unused imports - reportFetcher now only reads from database
// Removed import for deleted comprehensiveDataFetcher.js

/**
 * Helper function to calculate hold duration from agent history
 * @param {Array} agentHistory - Array of agent history events
 * @param {number|string} hangupTime - Call hangup time
 * @returns {string} - Hold duration in seconds or empty string
 */
function calculateHoldDurationFromHistory(agentHistory, hangupTime) {
  if (!Array.isArray(agentHistory) || agentHistory.length === 0) {
    return '';
  }
  
  // Local helper function for timestamp formatting to avoid scope issues
  function formatTimeForLog(ts) {
    if (ts == null || ts === '') return '';
    const ms = ts < 10_000_000_000 ? ts * 1000 : ts; // epoch secâ†’ms
    return new Date(ms).toLocaleString('en-GB', { timeZone: 'Asia/Dubai' });
  }
  
  let totalHoldDuration = 0;
  let holdPairs = [];
  
  // Find all hold_start and hold_stop pairs
  for (let i = 0; i < agentHistory.length; i++) {
    const event = agentHistory[i];
    
    if (!event || !event.event) continue;
    
    // Check for hold_start event
    if (event.event === 'hold_start' && event.last_attempt) {
      const holdStartTime = event.last_attempt;
      let holdEndTime = null;
      let matchFound = false;
      
      // Search forward for the matching hold_stop event
      for (let j = i + 1; j < agentHistory.length; j++) {
        const nextEvent = agentHistory[j];
        if (!nextEvent || !nextEvent.event) continue;
        
        if (nextEvent.event === 'hold_stop' && nextEvent.last_attempt) {
          holdEndTime = nextEvent.last_attempt;
          matchFound = true;
          break;
        }
        
        // If we encounter another hold_start before finding a hold_stop,
        // this means the previous hold_start didn't have a matching hold_stop
        // (could happen in case of system errors or incomplete data)
        if (nextEvent.event === 'hold_start') {
          break;
        }
      }
      
      // If no matching hold_stop found, use hangup_time as fallback
      if (!matchFound && hangupTime) {
        holdEndTime = typeof hangupTime === 'number' ? hangupTime : parseFloat(hangupTime);
      }
      
      // Add valid hold pair to our collection
      if (holdStartTime && holdEndTime && holdEndTime > holdStartTime) {
        const duration = holdEndTime - holdStartTime;
        holdPairs.push({
          start: holdStartTime,
          end: holdEndTime,
          duration: duration,
          agent: event.ext || '',
          agentName: `${event.first_name || ''} ${event.last_name || ''}`.trim()
        });
        totalHoldDuration += duration;
      }
    }
  }
  
  // Log details of hold periods for debugging
  if (holdPairs.length > 0) {
    console.log(`Hold duration calculation: Found ${holdPairs.length} hold periods totaling ${totalHoldDuration.toFixed(2)} seconds`);
    
    // Log details of each hold period
    holdPairs.forEach((pair, index) => {
      console.log(`Hold period ${index + 1}: Agent ${pair.agentName} (${pair.agent}) - ${formatTimeForLog(pair.start)} to ${formatTimeForLog(pair.end)} (${pair.duration.toFixed(2)} seconds)`);
    });
  }
  
  // Return the total hold duration in seconds (formatted to 2 decimal places)
  return totalHoldDuration > 0 ? totalHoldDuration.toFixed(2) : '';
}

/**
 * Detect transfer events in agent history
 * @param {Array} agentHistory - Agent history array
 * @param {string} callType - Call type ('inbound' or 'outbound')
 * @param {Object} call - Call object for logging purposes
 * @returns {Object} - Transfer information { transfer_event: boolean, transfer_extension: string, transfer_queue_extension: string, transfer_type: string }
 */
function detectTransferEvents(agentHistory, callType, call = {}) {
  if (!Array.isArray(agentHistory) || agentHistory.length === 0) {
    return {
      transfer_event: false,
      transfer_extension: null,
      transfer_queue_extension: null,
      transfer_type: null
    };
  }
  
  // Default result
  const result = {
    transfer_event: false,
    transfer_extension: null,
    transfer_queue_extension: null,
    transfer_type: null
  };

  const normalizedCallType = (callType || '').toLowerCase();

  // For inbound calls: look for type:attended, event:transfer
  // For outbound calls: look for type:transfer, event:transfer
  // For campaign calls: look for type:Transfer (now case-insensitive)
  const transferEvents = agentHistory.filter(event => {
    if (!event) return false;

    const type = (event.type  || '').toLowerCase();
    const evt  = (event.event || '').toLowerCase();

    if (normalizedCallType === 'inbound') {
      // Inbound calls use type="attended" and event="transfer" for actual transfers
      return type === 'attended' && evt === 'transfer';
    } else if (normalizedCallType === 'outbound') {
      // Outbound calls can use either type="transfer" OR type="attended" with event="transfer"
      return ((type === 'transfer' || type === 'attended') && evt === 'transfer');
    } else if (normalizedCallType === 'campaign') {
      // Campaign calls use type="Transfer" (capital T originally), match case-insensitively
      return type === 'transfer';
    }
    
    return false;
  });
  
  if (transferEvents.length > 0) {
    // Use the last transfer event (in case there are multiple)
    const lastTransferEvent = transferEvents[transferEvents.length - 1];
    
    result.transfer_event = true;
    const rawExtension = lastTransferEvent.ext || lastTransferEvent.extension || null;
    const extStr = rawExtension != null ? String(rawExtension) : null;

    // Set transfer_type based on normalized call type
    result.transfer_type =
      normalizedCallType === 'inbound'  ? 'inbound_transfer'  :
      normalizedCallType === 'outbound' ? 'outbound_transfer' :
      normalizedCallType === 'campaign' ? 'campaign_transfer' :
      'transfer';
    
    // Check if this is a transfer to a queue extension (8000-8999)
    if (
      extStr &&
      extStr.startsWith('8') &&
      extStr.length === 4 &&
      !Number.isNaN(parseInt(extStr, 10)) &&
      parseInt(extStr, 10) >= 8000 &&
      parseInt(extStr, 10) <= 8999
    ) {
      // For queue extensions, set both transfer_queue_extension and transfer_extension
      // transfer_extension will be set later by matching with inbound calls
      result.transfer_queue_extension = extStr;
      result.transfer_extension = extStr; // Ensure it's not null
      console.log(`📊 Queue extension transfer detected: ${extStr}`);
      
      // Enhanced logging for queue extension mapping
      const queueToCalleeExtensionMap = {
        '8000': '7020', '8001': '7014', '8002': '7015', '8003': '7016', '8004': '7012',
        '8005': '7017', '8006': '7018', '8007': '7019', '8008': '7021', '8009': '7034',
        '8010': '7023', '8011': '7028', '8012': '7029', '8013': '7031', '8014': '7033',
        '8015': '7030', '8016': '7013', '8017': '7011', '8018': '7010', '8019': '7008'
      };
      
      const expectedCalleeId = queueToCalleeExtensionMap[extStr];
      if (expectedCalleeId) {
        console.log(`📊 MAPPING FOUND: Queue extension ${extStr} maps to callee_id_number ${expectedCalleeId}`);
        console.log(`📊 COMPLETE MAPPING TABLE:`);
        Object.entries(queueToCalleeExtensionMap).forEach(([queue, callee]) => {
          const indicator = queue === extStr ? ' ← CURRENT' : '';
          console.log(`   ${queue} -> ${callee}${indicator}`);
        });
        
        // Special logging for 8001 -> 7014 mapping
        if (extStr === '8001' && expectedCalleeId === '7014') {
          const callId = call.callid || call.call_id || call.id || 'unknown';
          console.log(`📊 SPECIAL CASE: Processing 8001 -> 7014 mapping for ${normalizedCallType} call ${callId}`);
          console.log(`📊 EXPECTED RESULT: Should find inbound call with callee_id=7014 and extract agent extension (expected: 1002)`);
        }
      } else {
        console.log(`📊 MAPPING ERROR: No callee_id mapping found for queue extension ${extStr}`);
        console.log(`📊 AVAILABLE MAPPINGS: ${Object.keys(queueToCalleeExtensionMap).join(', ')}`);
      }
    } else {
      // For regular extensions, set only transfer_extension
      result.transfer_extension = extStr;
    }
    
    // Log transfer event details
    const callId = call.callid || call.call_id || call.id || 'unknown';
    const logLabel = normalizedCallType
      ? `${normalizedCallType.toUpperCase()} TRANSFER DETECTED`
      : 'TRANSFER DETECTED';

    console.log(`📊 ${logLabel}: Call ID ${callId}, Extension: ${extStr}`);
    console.log(`   - Type: ${lastTransferEvent.type}`);
    console.log(`   - Event: ${lastTransferEvent.event}`);
    console.log(`   - Extension: ${result.transfer_extension}`);
    console.log(`   - Queue Extension: ${result.transfer_queue_extension || 'N/A'}`);
    console.log(`   - Last Attempt: ${lastTransferEvent.last_attempt}`);
  }
  
  return result;
}


// fetchTransferredCalls function has been removed as it's no longer needed

const MAX_RETRIES = 3;

const ENDPOINTS = {
  // Raw CDRs
  cdrs: '/api/v2/reports/cdrs',
  
  // Queue-specific CDR summaries
  queueCalls: '/api/v2/reports/queues_cdrs',                 // inbound queues
  queueOutboundCalls: '/api/v2/reports/queues_outbound_cdrs', // outbound queues

  // Campaign dialer lead activity
  campaignsActivity: '/api/v2/reports/campaigns/leads/history',
  
  // Final report search (handled by our custom endpoint)
  search: '/api/reports/search'
};

// Simple in-memory cache (per Node process). In production replace with Redis.
const CACHE_TTL = ms('5m');          // 5 minutes
const reportCache = new Map();       // Map<cacheKey,{expires:number,data:object[]}>

// Generate a unique key from report + tenant + window params.
function makeCacheKey(report, tenant, params) {
  const { startDate = '', endDate = '' } = params || {};
  return `${report}|${tenant}|${startDate}|${endDate}`;
}

/**
 * Convert an array of plain objects to a CSV string.
 * Borrowed from agentStatus.js to avoid new deps.
 */
function toCsv(records, delimiter = ',') {
  if (!records.length) return '';
  const header = Object.keys(records[0]).join(delimiter);
  const rows = records.map(r =>
    Object.values(r)
      .map(v => {
        if (v == null) return '';
        const str = String(v);
        return str.includes(delimiter) || str.includes('\n') || str.includes('"')
          ? `"${str.replace(/"/g, '""')}"` // RFC4180 escaping
          : str;
      })
      .join(delimiter)
  );
  return [header, ...rows].join('\n');
}

/**
 * Extract CSAT value from agent history
 * @param {Array} agentHistory - Array of agent history events
 * @returns {string} - CSAT ext value or empty string if not found
 */
function extractCSATFromAgentHistory(agentHistory) {
  if (!Array.isArray(agentHistory) || agentHistory.length === 0) {
    return '';
  }
  
  // Find CSAT events in agent history
  for (const event of agentHistory) {
    if (event && event.type === 'CSAT' && event.event === 'CSAT customer' && event.ext) {
      return String(event.ext);
    }
  }
  
  return '';
}

/**
 * Normalize a raw phone string to E.164, then return the country name.
 * - Handles 00 / 011 international prefixes.
 * - Keeps national numbers and parses them with a defaultCountry.
 * - Avoids fragile hard-coded rewrites.
 *
 * @param {string} raw - Raw input (may contain 00..., 011..., 0-leading, spaces, etc.)
 * @param {string} [defaultCountry="AE"] - Your main market (AE / IN / ...). Used when no +/00/011 prefix.
 * @returns {string} Country name or '' if unknown/invalid.
 */
function extractCountryFromPhoneNumber(raw, defaultCountry = "AE") {
  if (!raw || typeof raw !== "string") {
    return "";
  }

  // 1) Keep '+'; keep digits; drop everything else.
  let s = raw.replace(/[^\d+]/g, "");
  

  // 2) Normalize international prefixes BEFORE stripping leading zeros.
  //    Handle specific problematic patterns first
  if (s.startsWith("000787")) {
    s = "+787" + s.slice(6); // Puerto Rico - remove extra zeros
  } else if (s.startsWith("000675")) {
    s = "+675" + s.slice(6); // Papua New Guinea
  } else if (s.startsWith("00067")) {
    s = "+67" + s.slice(5); // Various Pacific islands
  } else if (s.startsWith("0091")) {
    s = "+91" + s.slice(4); // India
  } else if (s.startsWith("0092")) {
    s = "+92" + s.slice(4); // Pakistan
  } else if (s.startsWith("0093")) {
    s = "+233" + s.slice(5); // Ghana
  } else if (s.startsWith("00235")) {
    s = "+235" + s.slice(5); // Chad
  } else if (s.startsWith("00237")) {
    s = "+237" + s.slice(5); // Cameroon
  } else if (s.startsWith("0061")) {
    s = "+61" + s.slice(4); // Australia - handle all 0061 patterns
  } else if (s.startsWith("00234")) {
    s = "+234" + s.slice(5); // Nigeria
  } else if (s.startsWith("00233")) {
    s = "+233" + s.slice(5); // Ghana
  } else if (s.startsWith("00235")) {
    s = "+235" + s.slice(5); // Chad
  } else if (s.startsWith("00237")) {
    s = "+237" + s.slice(5); // Cameroon
  } else if (s.startsWith("00238")) {
    s = "+238" + s.slice(5); // Cape Verde
  } else if (s.startsWith("00239")) {
    s = "+239" + s.slice(5); // São Tomé and Príncipe
  } else if (s.startsWith("00240")) {
    s = "+240" + s.slice(5); // Equatorial Guinea
  } else if (s.startsWith("00241")) {
    s = "+241" + s.slice(5); // Gabon
  } else if (s.startsWith("00242")) {
    s = "+242" + s.slice(5); // Republic of the Congo
  } else if (s.startsWith("00243")) {
    s = "+243" + s.slice(5); // Democratic Republic of the Congo
  } else if (s.startsWith("00244")) {
    s = "+244" + s.slice(5); // Angola
  } else if (s.startsWith("00245")) {
    s = "+245" + s.slice(5); // Guinea-Bissau
  } else if (s.startsWith("00246")) {
    s = "+246" + s.slice(5); // British Indian Ocean Territory
  } else if (s.startsWith("00247")) {
    s = "+247" + s.slice(5); // Ascension Island
  } else if (s.startsWith("00248")) {
    s = "+248" + s.slice(5); // Seychelles
  } else if (s.startsWith("00249")) {
    s = "+249" + s.slice(5); // Sudan
  } else if (s.startsWith("00250")) {
    s = "+250" + s.slice(5); // Rwanda
  } else if (s.startsWith("00251")) {
    s = "+251" + s.slice(5); // Ethiopia
  } else if (s.startsWith("0000252634456618")) {
    s = "+252" + s.slice(7); // Somalia - remove extra zeros
  } else if (s.startsWith("00070") || s.startsWith("00071") || s.startsWith("00072") || 
             s.startsWith("00073") || s.startsWith("00074") || s.startsWith("00075") || 
             s.startsWith("00076") || s.startsWith("00078") || s.startsWith("00079")) {
    s = "+7" + s.slice(4); // Russia mobile - remove extra zeros
  } else if (s.startsWith("000348")) {
    s = "+34" + s.slice(5); // Spain - remove extra zeros
  } else if (s.startsWith("000604")) {
    s = "+60" + s.slice(5); // Malaysia - remove extra zeros
  } else if (s.startsWith("007489")) {
    s = "+7" + s.slice(4); // Russia mobile - remove extra zeros
  } else if (s.startsWith("00891")) {
    s = "+91" + s.slice(4); // India mobile - remove extra zeros (00891 -> +91)
  } else if (s.startsWith("000787")) {
    s = "+787" + s.slice(6); // Puerto Rico - remove extra zeros
  } else if (s.startsWith("00254")) {
    s = "+254" + s.slice(5); // Kenya
  } else if (s.startsWith("00255")) {
    s = "+255" + s.slice(5); // Tanzania
  } else if (s.startsWith("00256")) {
    s = "+256" + s.slice(5); // Uganda
  } else if (s.startsWith("00257")) {
    s = "+257" + s.slice(5); // Burundi
  } else if (s.startsWith("00258")) {
    s = "+258" + s.slice(5); // Mozambique
  } else if (s.startsWith("00260")) {
    s = "+260" + s.slice(5); // Zambia
  } else if (s.startsWith("00261")) {
    s = "+261" + s.slice(5); // Madagascar
  } else if (s.startsWith("00262")) {
    s = "+262" + s.slice(5); // Réunion/Mayotte
  } else if (s.startsWith("00263")) {
    s = "+263" + s.slice(5); // Zimbabwe
  } else if (s.startsWith("00264")) {
    s = "+264" + s.slice(5); // Namibia
  } else if (s.startsWith("00265")) {
    s = "+265" + s.slice(5); // Malawi
  } else if (s.startsWith("00266")) {
    s = "+266" + s.slice(5); // Lesotho
  } else if (s.startsWith("00267")) {
    s = "+267" + s.slice(5); // Botswana
  } else if (s.startsWith("00268")) {
    s = "+268" + s.slice(5); // Eswatini
  } else if (s.startsWith("00269")) {
    s = "+269" + s.slice(5); // Comoros
  } else if (s.startsWith("0044")) {
    s = "+44" + s.slice(4); // United Kingdom
  } else if (s.startsWith("0047")) {
    s = "+47" + s.slice(4); // Norway
  } else if (s.startsWith("0046")) {
    s = "+46" + s.slice(4); // Sweden
  } else if (s.startsWith("0055")) {
    s = "+55" + s.slice(4); // Brazil
  } else if (s.startsWith("0056")) {
    s = "+56" + s.slice(4); // Chile
  } else if (s.startsWith("0057")) {
    s = "+57" + s.slice(4); // Colombia
  } else if (s.startsWith("0058")) {
    s = "+58" + s.slice(4); // Venezuela
  } else if (s.startsWith("0059")) {
    s = "+59" + s.slice(4); // Various South American
  } else if (s.startsWith("0066")) {
    s = "+66" + s.slice(4); // Thailand
  } else if (s.startsWith("0067")) {
    s = "+67" + s.slice(4); // Various Pacific
  } else if (s.startsWith("0068")) {
    s = "+68" + s.slice(4); // Various Pacific
  } else if (s.startsWith("0069")) {
    s = "+69" + s.slice(4); // Various Pacific
  } else if (s.startsWith("0070")) {
    s = "+70" + s.slice(4); // Various
  } else if (s.startsWith("0071")) {
    s = "+71" + s.slice(4); // Various
  } else if (s.startsWith("0072")) {
    s = "+72" + s.slice(4); // Various
  } else if (s.startsWith("0073")) {
    s = "+73" + s.slice(4); // Various
  } else if (s.startsWith("0074")) {
    s = "+74" + s.slice(4); // Various
  } else if (s.startsWith("0075")) {
    s = "+75" + s.slice(4); // Various
  } else if (s.startsWith("0076")) {
    s = "+76" + s.slice(4); // Various
  } else if (s.startsWith("0077")) {
    s = "+77" + s.slice(4); // Kazakhstan
  } else if (s.startsWith("0078")) {
    s = "+78" + s.slice(4); // Various
  } else if (s.startsWith("0091")) {
    s = "+91" + s.slice(4); // India
  } else if (s.startsWith("0092")) {
    s = "+92" + s.slice(4); // Pakistan
  } else if (s.startsWith("0093")) {
    s = "+93" + s.slice(4); // Afghanistan
  } else if (s.startsWith("0094")) {
    s = "+94" + s.slice(4); // Sri Lanka
  } else if (s.startsWith("0095")) {
    s = "+95" + s.slice(4); // Myanmar
  } else if (s.startsWith("0096")) {
    s = "+96" + s.slice(4); // Various Middle East
  } else if (s.startsWith("0097")) {
    s = "+97" + s.slice(4); // Various Middle East
  } else if (s.startsWith("0098")) {
    s = "+98" + s.slice(4); // Iran
  } else if (s.startsWith("0099")) {
    s = "+99" + s.slice(4); // Various
  } else if (s.startsWith("00216")) {
    s = "+216" + s.slice(5); // Tunisia
  } else if (s.startsWith("00225")) {
    s = "+225" + s.slice(5); // Côte d'Ivoire
  } else if (s.startsWith("00243")) {
    s = "+243" + s.slice(5); // Democratic Republic of Congo
  } else if (s.startsWith("00258")) {
    s = "+258" + s.slice(5); // Mozambique
  } else if (s.startsWith("00277")) {
    s = "+277" + s.slice(5); // Various
  } else if (s.startsWith("00404")) {
    s = "+404" + s.slice(5); // Georgia (mobile)
  } else if (s.startsWith("00590")) {
    s = "+590" + s.slice(5); // Guadeloupe
  } else if (s.startsWith("00855")) {
    s = "+855" + s.slice(5); // Cambodia
  } else if (s.startsWith("00889")) {
    s = "+889" + s.slice(5); // Various (but often invalid)
  } else if (s.startsWith("00964")) {
    s = "+964" + s.slice(5); // Iraq
  } else if (s.startsWith("00801")) {
    return ""; // Invalid international service codes
  } else if (s.startsWith("00891")) {
    return ""; // Invalid premium rate codes
  } else if (s.startsWith("00787")) {
    s = "+787" + s.slice(5); // Puerto Rico (US territory)
  } else if (s.startsWith("00706")) {
    s = "+706" + s.slice(5); // Various
  } else if (s.startsWith("00588")) {
    s = "+588" + s.slice(5); // Various Pacific
  } else if (s.startsWith("00755")) {
    s = "+755" + s.slice(5); // Various
  } else if (s.startsWith("0075")) {
    s = "+75" + s.slice(4); // Kazakhstan mobile
  } else if (s.startsWith("00143")) {
    s = "+1" + s.slice(5); // North America (001 + 43)
  } else if (s.startsWith("00125")) {
    s = "+1" + s.slice(5); // North America (001 + 25)  
  } else if (s.startsWith("00197")) {
    s = "+1" + s.slice(5); // North America (001 + 97)
  } else if (s.startsWith("00170")) {
    s = "+1" + s.slice(5); // North America (001 + 70)
  } else if (s.startsWith("00162")) {
    s = "+1" + s.slice(5); // North America (001 + 62)
  } else if (s.startsWith("00120")) {
    s = "+1" + s.slice(5); // North America (001 + 20)
  } else if (s.startsWith("00176")) {
    s = "+1" + s.slice(5); // North America (001 + 76)
  } else if (s.startsWith("00128")) {
    s = "+1" + s.slice(5); // North America (001 + 28)
  } else if (s.startsWith("00124")) {
    s = "+1" + s.slice(5); // North America (001 + 24)
  } else if (s.startsWith("00125")) {
    s = "+1" + s.slice(5); // North America (001 + 25)
  } else if (s.startsWith("00106")) {
    s = "+1" + s.slice(5); // North America (001 + 06)
  } else if (s.startsWith("00135")) {
    s = "+1" + s.slice(5); // North America (001 + 35)
  } else if (s.startsWith("00152")) {
    s = "+1" + s.slice(5); // North America (001 + 52)
  } else if (s.startsWith("00109")) {
    s = "+1" + s.slice(5); // North America (001 + 09)
  } else if (s.startsWith("00191")) {
    s = "+1" + s.slice(5); // North America (001 + 91)
  } else if (s.startsWith("00055")) {
    s = "+55" + s.slice(5); // Brazil (000 + 55)
  } else if (s.startsWith("00058")) {
    s = "+58" + s.slice(5); // Venezuela (000 + 58)
  } else if (s.startsWith("00891")) {
    return ""; // Invalid premium rate codes
  } else if (s.startsWith("0089")) {
    s = "+89" + s.slice(4); // Various
  } else if (s.startsWith("001")) {
    s = "+1" + s.slice(3); // North America
  } else if (s.startsWith("007")) {
    s = "+7" + s.slice(3); // Russia/Kazakhstan
  } else if (s.startsWith("00")) {
    s = "+" + s.slice(2); // General case
  } else if (s.startsWith("011")) {
    s = "+" + s.slice(3); // US-style intl access
  }
  

  // 3) If it still doesn't start with '+', DO NOT strip all zeros blindly.
  //    Let libphonenumber parse it as a national number using defaultCountry.
  //    (Many countries need a leading '0' for national format.)
  //    Only if it's obviously an international-without-plus (rare), we'll add '+' later.

  // 4) Quick guard against garbage and extremely long numbers
  const onlyDigits = s.replace(/\D/g, "");
  if (onlyDigits.length < 5 || onlyDigits.length > 18) {
    return "";
  }
  
  // Filter out obviously invalid patterns
  if (s === "00" || s === "000" || s === "0000") {
    return "";
  }
  if (s.startsWith("00800") || s.startsWith("00801") || s.startsWith("00889") || 
      s.startsWith("00808") || s.startsWith("00880") || s.startsWith("00881") ||
      s.startsWith("00882") || s.startsWith("00883") || s.startsWith("00884") ||
      s.startsWith("00885") || s.startsWith("00886") || s.startsWith("00887") ||
      s.startsWith("00888") || s.startsWith("00890") || s.startsWith("00891") ||
      s.startsWith("00892") || s.startsWith("00893") || s.startsWith("00894") ||
      s.startsWith("00895") || s.startsWith("00896") || s.startsWith("00897") ||
      s.startsWith("00898") || s.startsWith("00899")) {
    return ""; // Invalid service codes
  }

  // 5) Try libphonenumber-js parsing first
  let pn =
    s.startsWith("+")
      ? parsePhoneNumberFromString(s)                   // international
      : parsePhoneNumberFromString(s, defaultCountry);  // national

  // 6) If parsing failed BUT it looks like an international number missing '+',
  //    try adding '+'
  if (!pn && !s.startsWith("+") && onlyDigits.length >= 7) {
    pn = parsePhoneNumberFromString("+" + onlyDigits);
  }

  // 7) If libphonenumber worked, use it (even if number format is invalid)
  if (pn && pn.country) {
    const ISO = pn.country;
    if (ISO) {
      // Define country names mapping
      const COUNTRY_NAMES = {
        AE: "United Arab Emirates", IN: "India", GB: "United Kingdom", US: "United States", CA: "Canada",
        AU: "Australia", DE: "Germany", FR: "France", IT: "Italy", ES: "Spain", NL: "Netherlands",
        BE: "Belgium", CH: "Switzerland", AT: "Austria", SE: "Sweden", NO: "Norway", DK: "Denmark",
        FI: "Finland", PL: "Poland", CZ: "Czech Republic", HU: "Hungary", RO: "Romania", BG: "Bulgaria",
        HR: "Croatia", SI: "Slovenia", SK: "Slovakia", LT: "Lithuania", LV: "Latvia", EE: "Estonia",
        IE: "Ireland", PT: "Portugal", GR: "Greece", CY: "Cyprus", MT: "Malta", LU: "Luxembourg",
        BR: "Brazil", MX: "Mexico", AR: "Argentina", CL: "Chile", CO: "Colombia", PE: "Peru",
        VE: "Venezuela", UY: "Uruguay", PY: "Paraguay", BO: "Bolivia", EC: "Ecuador",
        RU: "Russia", UA: "Ukraine", BY: "Belarus", KZ: "Kazakhstan", UZ: "Uzbekistan", KG: "Kyrgyzstan",
        TJ: "Tajikistan", TM: "Turkmenistan", AM: "Armenia", AZ: "Azerbaijan", GE: "Georgia",
        MD: "Moldova", EG: "Egypt", DZ: "Algeria", SA: "Saudi Arabia", TR: "Turkey", IL: "Israel",
        JO: "Jordan", LB: "Lebanon", SY: "Syria", IQ: "Iraq", IR: "Iran", AF: "Afghanistan",
        PK: "Pakistan", BD: "Bangladesh", LK: "Sri Lanka", NP: "Nepal", BT: "Bhutan", MV: "Maldives",
        ZA: "South Africa", NG: "Nigeria", KE: "Kenya", GH: "Ghana", GM: "Gambia", ET: "Ethiopia",
        TZ: "Tanzania", UG: "Uganda", ZW: "Zimbabwe", ZM: "Zambia", MW: "Malawi", MZ: "Mozambique",
        BW: "Botswana", NA: "Namibia", SZ: "Eswatini", LS: "Lesotho", TN: "Tunisia", NR: "Nauru",
        MM: "Myanmar", LA: "Laos", KH: "Cambodia", BN: "Brunei", MN: "Mongolia", KP: "North Korea",
        SC: "Seychelles", MU: "Mauritius", FJ: "Fiji", TO: "Tonga", WS: "Samoa", VU: "Vanuatu",
        PG: "Papua New Guinea", SB: "Solomon Islands", NC: "New Caledonia", PF: "French Polynesia",
        LR: "Liberia", MA: "Morocco", NE: "Niger", TD: "Chad", KW: "Kuwait", TG: "Togo",
        AO: "Angola", CI: "Côte d'Ivoire", QA: "Qatar", BH: "Bahrain", SO: "Somalia", 
        SN: "Senegal", TL: "Timor-Leste", CV: "Cape Verde", ST: "São Tomé and Príncipe",
        GQ: "Equatorial Guinea", GA: "Gabon", CG: "Republic of the Congo", CD: "Democratic Republic of the Congo",
        GW: "Guinea-Bissau", IO: "British Indian Ocean Territory", AC: "Ascension Island",
        SD: "Sudan", RW: "Rwanda", DJ: "Djibouti", BI: "Burundi", RE: "Réunion",
        YT: "Mayotte", MG: "Madagascar", KM: "Comoros", TH: "Thailand", VN: "Vietnam",
        KH: "Cambodia", LA: "Laos", MM: "Myanmar", MY: "Malaysia", SG: "Singapore",
        ID: "Indonesia", PH: "Philippines", TW: "Taiwan", HK: "Hong Kong", MO: "Macau",
        KR: "South Korea", JP: "Japan", CN: "China", MN: "Mongolia", KP: "North Korea",
        KZ: "Kazakhstan", UZ: "Uzbekistan", KG: "Kyrgyzstan", TJ: "Tajikistan", TM: "Turkmenistan",
        AF: "Afghanistan", PK: "Pakistan", BD: "Bangladesh", LK: "Sri Lanka", NP: "Nepal",
        BT: "Bhutan", MV: "Maldives", IR: "Iran", IQ: "Iraq", SY: "Syria", LB: "Lebanon",
        JO: "Jordan", IL: "Israel", PS: "Palestine", YE: "Yemen", OM: "Oman", 
        BR: "Brazil", AR: "Argentina", CL: "Chile", CO: "Colombia", VE: "Venezuela",
        UY: "Uruguay", PY: "Paraguay", BO: "Bolivia", EC: "Ecuador", PE: "Peru",
        GY: "Guyana", SR: "Suriname", GF: "French Guiana", GP: "Guadeloupe",
        MQ: "Martinique", BL: "Saint Barthélemy", MF: "Saint Martin"
      };
      const countryName = COUNTRY_NAMES[ISO] || ISO;
      return countryName;
    }
  }

  // 8) Fallback: Handle UAE numbers explicitly (when libphonenumber fails)
  if (s.startsWith("+971") || (s.startsWith("971") && onlyDigits.length >= 12)) {
    return "United Arab Emirates";
  }
  
  // Handle UAE national format numbers
  if (!s.startsWith("+") && s.startsWith("065") && s.length >= 9) {
    return "United Arab Emirates";
  }
  
  // Handle other UAE national formats
  if (!s.startsWith("+") && (s.startsWith("05") || s.startsWith("02") || s.startsWith("03") || 
      s.startsWith("04") || s.startsWith("06") || s.startsWith("07")) && s.length >= 8 && s.length <= 9) {
    return "United Arab Emirates";
  }

  // 9) Additional fallback patterns for specific problematic numbers
  // Handle Russia mobile numbers that might not be caught
  if (s.startsWith("+7973") || s.startsWith("+7904") || s.startsWith("+7150") ||
      s.startsWith("7973") || s.startsWith("7904") || s.startsWith("7150") ||
      s.startsWith("+7954") || s.startsWith("7954")) {
    return "Russia";
  }
  
  // Handle Brazil numbers
  if (s.startsWith("+5543") || s.startsWith("+5584") ||
      s.startsWith("5543") || s.startsWith("5584")) {
    return "Brazil";
  }
  
  // Handle Venezuela numbers
  if (s.startsWith("+588") || s.startsWith("588")) {
    return "Venezuela";
  }
  
  // Handle India mobile numbers
  if (s.startsWith("+91844") || s.startsWith("+91849") ||
      s.startsWith("91844") || s.startsWith("91849")) {
    return "India";
  }
  
  // Handle Kazakhstan mobile numbers
  if (s.startsWith("+7576") || s.startsWith("+7765") ||
      s.startsWith("7576") || s.startsWith("7765")) {
    return "Kazakhstan";
  }
  
  // Handle Puerto Rico numbers
  if (s.startsWith("+787") || s.startsWith("787")) {
    return "Puerto Rico";
  }
  
  // Handle North America numbers (US/Canada)
  if (s.startsWith("+1") || (s.startsWith("1") && s.length >= 11)) {
    return "United States";
  }
  
  // Handle Australia numbers
  if (s.startsWith("+61") || (s.startsWith("61") && s.length >= 10)) {
    return "Australia";
  }
  
  // Handle UK numbers
  if (s.startsWith("+44") || (s.startsWith("44") && s.length >= 10)) {
    return "United Kingdom";
  }
  
  // Handle Italy numbers
  if (s.startsWith("+39") || (s.startsWith("39") && s.length >= 10)) {
    return "Italy";
  }
  
  // Handle Spain numbers
  if (s.startsWith("+34") || (s.startsWith("34") && s.length >= 9)) {
    return "Spain";
  }
  
  // Handle Malaysia numbers
  if (s.startsWith("+60") || (s.startsWith("60") && s.length >= 9)) {
    return "Malaysia";
  }
  
  // Handle special satellite/international numbers
  if (s.startsWith("+881") || s.startsWith("+882") || s.startsWith("+883") ||
      s.startsWith("881") || s.startsWith("882") || s.startsWith("883")) {
    return "International Networks";
  }
  
  if (s.startsWith("+888") || s.startsWith("888")) {
    return "International Shared Cost Service";
  }

  // 10) If we get here, nothing worked
  return "";
}

/**
 * Fetch all data from APIs and populate database tables.
 * This function handles complete pagination and stores all records in the database.
 * 
 * @param {string} tenant - Tenant identifier
 * @param {Object} params - Query parameters (startDate, endDate, etc.)
 * @returns {Promise<Object>} - Summary of fetched and stored records
 */
export async function populateDatabase(tenant, params = {}) {
  console.log(' 🎉 Starting database population from all API endpoints...');
  // Import the API data fetcher
  const { fetchAllAPIsAndPopulateDB } = await import('./apiDataFetcher.js');
  return await fetchAllAPIsAndPopulateDB(tenant, params);
}

/**
 * Populate a specific endpoint in the database
 * @param {string} endpoint - Endpoint to populate
 * @param {string} tenant - Tenant identifier
 * @param {Object} params - Query parameters
 * @returns {Promise<Object>} - Result for the specific endpoint
 */
export async function populateEndpoint(endpoint, tenant, params = {}) {
  console.log(`📊 Populating specific endpoint: ${endpoint}`);
  return await fetchAndStoreEndpoint(endpoint, tenant, params);
}

/**
 * Normalize phone number for matching (remove non-digits, handle extensions)
 * Enhanced to handle various phone number formats and extensions properly
 * @param {string} phoneNumber - Phone number to normalize
 * @returns {string} - Normalized phone number
 */
function normalizePhoneNumber(phoneNumber) {
  if (!phoneNumber || typeof phoneNumber !== 'string') {
    return '';
  }
  
  // Remove all non-digit characters
  let normalized = phoneNumber.replace(/\D/g, '');
  
  // Remove leading zeros
  normalized = normalized.replace(/^0+/, '');
  
  // Handle extensions by taking last 10 digits if longer than 10
  // This helps match extensions with full numbers
  if (normalized.length > 10) {
    normalized = normalized.slice(-10);
  }
  
  return normalized;
}

/**
 * Process and sanitize CDR record data for clean display
 * @param {Object} record - Raw CDR record
 * @returns {Object} - Processed CDR record with sanitized fields
 */
function processCDRRecord(record) {
  const processed = {};
  
  // Basic identification
  processed.Type = 'CDR';
  processed['Agent name'] = record.agent_name || record.agent ;
  processed.Extension = record.caller_id_number || record.extension || '';
  processed['Queue / Campaign Name'] = record.queue_name || record.campaign_name ;
  
  // Timestamps - use formatTimestamp for proper local time display
  processed['Called Time'] = formatTimestamp(record.timestamp || record.called_time);
  
  // Calculate answered and hangup times from CDR timestamp and durations
  let answeredTime = '';
  let hangupTime = '';
  
  if (record.timestamp && record.ringing_seconds) {
    const callStartTime = record.timestamp;
    const ringingSec = parseInt(record.ringing_seconds) || 0;
    const durationSec = parseInt(record.duration_seconds) || 0;
    
    if (ringingSec > 0) {
      const answeredTimestamp = callStartTime + ringingSec;
      answeredTime = formatTimestamp(answeredTimestamp);
    }
    
    if (durationSec > 0) {
      const hangupTimestamp = callStartTime + durationSec;
      hangupTime = formatTimestamp(hangupTimestamp);
    }
  }
  
  processed['Answered time'] = answeredTime || formatTimestamp(record.answered_time || record.answer_time);
  processed['Hangup time'] = hangupTime || formatTimestamp(record.hangup_time || record.end_time);
  
  // Phone numbers and caller info
  processed['Caller ID Number'] = record.caller_id_number || '';
  processed['Caller ID / Lead Name'] = record.caller_id_name || record.lead_name || 'No';
  processed['Callee ID / Lead number'] = record.destination_number || record.callee_number || '';
  
  // Call durations - format as HH:MM:SS using CDR-specific fields
  processed['Wait Duration'] = formatDuration(record.ringing_seconds || record.wait_duration || record.queue_time);
  processed['Talk Duration'] = formatDuration(record.billing_seconds || record.duration_seconds || record.talk_duration || record.call_duration);
  
  // Calculate hold duration from agent_history events
  let holdDuration = '';
  if (Array.isArray(record.agent_history) && record.agent_history.length > 0) {
    const holdDurationSeconds = calculateHoldDurationFromHistory(record.agent_history, record.hangup_time);
    holdDuration = holdDurationSeconds;
    processed['Hold Duration'] = holdDurationSeconds ? formatDuration(parseFloat(holdDurationSeconds)) : '';
  } else {
    // Fallback to direct hold_duration field if agent_history is not available
    processed['Hold Duration'] = formatDuration(record.hold_duration);
  }
  
  // Agent and call details
  processed['Agent Hangup'] = record.agent_hangup || 'No';
  processed['Status'] = record.hangup_cause || record.status || '';
  processed['Agent Disposition'] = record.agent_disposition || '';
  processed.Sub_disp_1 = record.sub_disp_1 || record.sub_disposition_1 || '';
  processed.Sub_disp_2 = record.sub_disp_2 || record.sub_disposition_2 || '';
  processed.Sub_disp_3 = record.sub_disp_3 || record.sub_disposition_3 || '';
  
  // Additional fields
  // Extract follow_up_notes directly from the response data
  processed['Follow up notes'] = record.follow_up_notes || '';
  
  // Add System Disposition for CDR records - use the disposition from the record or default
  processed['System Disposition'] = record.disposition || '';

  processed.Recording = record.recording_url || record.recording || '';
  processed['Agent History'] = formatAgentHistory(record.agent_history);
  processed['Queue History'] = formatQueueHistory(record.queue_history);
  processed.Abandoned = record.abandoned || 'No';
  processed.Country = extractCountryFromPhoneNumber(record.destination_number || record.callee_number);
  // Use database CSAT value first, fallback to agent history extraction
  processed.CSAT = record.csat || extractCSATFromAgentHistory(record.agent_history) || '';
  processed['Call ID'] = record.call_id || record.callid || '';
  processed.Status = record.status || record.call_status || '';
  processed['Campaign Type'] = record.campaign_type || '';
  
  return processed;
}

/**
 * CDR matching using Queue Outbound Calls data with exact criteria:
 * 1. CDR caller_id_number must match outbound agent_ext
 * 2. CDR datetime must be between outbound called_time and hangup_time
 * @param {Object} cdrRecord - CDR record to check
 * @param {Map} outboundByExtension - Map of extensions to Queue Outbound Call arrays
 * @returns {boolean} - True if CDR matches a Queue Outbound Call
 */
function cdrMatchesOutboundOptimized(cdrRecord, outboundByExtension) {
  if (!cdrRecord || !outboundByExtension) {
    return false;
  }
  
  // Get CDR caller_id_number (normalized)
  const cdrCallerIdNumber = normalizePhoneNumber(cdrRecord.caller_id_number);
  // Get CDR timestamp (convert to milliseconds if needed)
  const cdrTimestamp = cdrRecord.timestamp;
  const cdrDateTime = new Date(cdrTimestamp < 1e10 ? cdrTimestamp * 1000 : cdrTimestamp);
  
  if (!cdrCallerIdNumber || !cdrTimestamp || isNaN(cdrDateTime.getTime())) {
    return false;
  }
  
  // Find Queue Outbound Calls with matching agent_ext
  const matchingOutboundCalls = outboundByExtension.get(cdrCallerIdNumber);
  if (!matchingOutboundCalls || matchingOutboundCalls.length === 0) {
    return false;
  }
  
  // Check if CDR datetime is between called_time and hangup_time of any matching outbound call
  return matchingOutboundCalls.some(outboundCall => {
    // Parse outbound call timestamps
    let outboundCalledTime, outboundHangupTime;
    
    if (typeof outboundCall.called_time === 'number') {
      const calledTimeMs = outboundCall.called_time < 1e10 ? outboundCall.called_time * 1000 : outboundCall.called_time;
      outboundCalledTime = new Date(calledTimeMs);
    } else {
      outboundCalledTime = new Date(outboundCall.called_time);
    }
    
    if (typeof outboundCall.hangup_time === 'number') {
      const hangupTimeMs = outboundCall.hangup_time < 1e10 ? outboundCall.hangup_time * 1000 : outboundCall.hangup_time;
      outboundHangupTime = new Date(hangupTimeMs);
    } else {
      outboundHangupTime = new Date(outboundCall.hangup_time);
    }
    
    if (isNaN(outboundCalledTime.getTime()) || isNaN(outboundHangupTime.getTime())) {
      return false;
    }
    
    // Check if CDR datetime is between outbound called_time and hangup_time
    const timeMatch = cdrDateTime >= outboundCalledTime && cdrDateTime <= outboundHangupTime;
    
    if (timeMatch) {
      console.log(`📊 CDR Match: ${cdrRecord.call_id || 'unknown'} matches Queue Outbound Call ${outboundCall.call_id || 'unknown'} (ext: ${cdrCallerIdNumber}, CDR time: ${cdrDateTime.toISOString()}, Outbound: ${outboundCalledTime.toISOString()} - ${outboundHangupTime.toISOString()})`);
      return true;
    }
    
    return false;
  });
}

/**
 * Unified function that fetches all APIs, populates database, and returns combined report
 * @param {string} tenant - Tenant identifier
 * @param {Object} params - Query parameters (startDate, endDate, etc.)
 * @returns {Promise<Object>} - Combined report with all data sorted by time
 */
export async function fetchUnifiedReport(tenant, params = {}) {
  console.log('🎉 Starting unified report fetch - APIs → Database → Combined Report');
  
  try {
    
    // Step 1: Check existing data, only populate if needed
    console.log('✅ Step 1: Checking existing database data...');
    
    // Check if we have recent data in database
    const existingData = await Promise.all([
      dbService.getRawCampaigns({}),
      dbService.getRawQueueInbound({}), 
      dbService.getRawQueueOutbound({}),
    ]);
    
    const totalExisting = existingData.reduce((sum, records) => sum + records.length, 0);
    const [campaignExisting, inboundExisting, outboundExisting] = existingData;
    
    console.log(`Found ${totalExisting} existing records in database:`, {
      campaigns: campaignExisting.length,
      inbound: inboundExisting.length, 
      outbound: outboundExisting.length,
      // cdrs: cdrExisting.length,
      // cdrs_all: cdrAllExisting.length
    });
    
    let populationResults = {};
    // Force fresh fetch if no data exists
    if (totalExisting === 0) {
      console.log('📊 No existing data found, fetching from APIs...');
      populationResults = await populateDatabase(tenant, params); // Pass date parameters to APIs
      console.log('📊 Database population completed:', populationResults);
    } else {
      console.log('📊 Using existing database data');
      populationResults = { message: 'Using existing data' };
    }
    
    // Step 2: Fetch filtered data from database tables (including cdrs_all for follow-up notes)
    console.log('✅ Step 2: Fetching filtered data from database tables...');
    
    const [campaignRecords, inboundRecords, outboundRecords] = await Promise.all([
      dbService.getRawCampaigns(params),
      dbService.getRawQueueInbound(params), 
      dbService.getRawQueueOutbound(params),
    ]);
    
    console.log(`✅ Fetched records: Campaign: ${campaignRecords.length}, Inbound: ${inboundRecords.length}, Outbound: ${outboundRecords.length} `);
    
    // We now extract follow-up notes directly from each record type
    console.log(`📊 Processing records to extract follow-up notes directly from each record...`);
    
    // Process and normalize records with proper data transformation
    let allRecords = [];
    
    // Process campaign records
    campaignRecords.forEach(row => {
      const record = { ...row.raw_data };
      
      // Apply campaign-specific data processing using dedicated function
      const processedRecord = processCampaignRecord(record);
      Object.assign(record, processedRecord);
      record._recordType = 'campaign';
      record.record_type = 'campaign'; // Also set for compatibility
      record.Type = 'Campaign'; // Ensure Type field is set for frontend styling
      
      // Extract follow_up_notes directly from the record
      const notes = record.follow_up_notes || '';
      if (notes) {
        record['Follow up notes'] = notes;
        const callId = record.call_id || record.callid;
        console.log(`📊 Found follow-up notes in campaign record ${callId}: "${notes.substring(0, 30)}${notes.length > 30 ? '...' : ''}"`);
      }
      
      allRecords.push(record);
    });
    
    // Process inbound queue records
    inboundRecords.forEach(row => {
      const record = { ...row.raw_data };
      // Apply inbound-specific data processing
      const processedRecord = processRecordData(record, 'queueCalls');
      Object.assign(record, processedRecord);
      record._recordType = 'inbound';
      record.record_type = 'inbound'; // Also set for compatibility
      record.Type = 'Inbound'; // Ensure Type field is set for frontend styling
      
      // Detect transfer events in agent_history for inbound calls
      if (Array.isArray(record.agent_history)) {
        const transferInfo = detectTransferEvents(record.agent_history, 'inbound', record);
        record.transfer_event = transferInfo.transfer_event;
        record.transfer_extension = transferInfo.transfer_extension;
        record.transfer_queue_extension = transferInfo.transfer_queue_extension;
        record.transfer_type = transferInfo.transfer_type;
        
        if (transferInfo.transfer_event) {
          console.log(`ðŸ”„ Inbound call ${record.callid || record.call_id} has transfer to extension ${transferInfo.transfer_extension}`);
        }
      }
      
      // Extract follow_up_notes directly from the record
      const notes = record.follow_up_notes || '';
      if (notes) {
        record['Follow up notes'] = notes;
        const callId = record.call_id || record.callid;
        console.log(`ðŸ“ Found follow-up notes in inbound record ${callId}: "${notes.substring(0, 30)}${notes.length > 30 ? '...' : ''}"`); 
      }
      
      allRecords.push(record);
    });
    
    // Process outbound queue records
    outboundRecords.forEach(row => {
      const record = { ...row.raw_data };
      // Apply outbound-specific data processing
      const processedRecord = processRecordData(record, 'queueOutboundCalls');
      Object.assign(record, processedRecord);
      record._recordType = 'outbound';
      record.record_type = 'outbound'; // Also set for compatibility
      record.Type = 'Outbound'; // Ensure Type field is set for frontend styling
      
      // Detect transfer events in agent_history for outbound calls
      if (Array.isArray(record.agent_history)) {
        const transferInfo = detectTransferEvents(record.agent_history, 'outbound', record);
        record.transfer_event = transferInfo.transfer_event;
        record.transfer_extension = transferInfo.transfer_extension;
        record.transfer_queue_extension = transferInfo.transfer_queue_extension;
        record.transfer_type = transferInfo.transfer_type;
        
        if (transferInfo.transfer_event) {
          console.log(`ðŸ”„ Outbound call ${record.callid || record.call_id} has transfer to extension ${transferInfo.transfer_extension}`);
        }
      }
      
      
      // Extract follow_up_notes directly from the record
      const followUpNotes = record.follow_up_notes || '';
      if (followUpNotes) {
        record['Follow up notes'] = followUpNotes;
        const callId = record.call_id || record.callid;
        console.log(`ðŸ“ Found follow-up notes in outbound record ${callId}: "${followUpNotes.substring(0, 30)}${followUpNotes.length > 30 ? '...' : ''}"`); 
      }
      
      allRecords.push(record);
    });
    
    // Step 3: Apply CDR filtering - only show CDRs matching outbound calls
    console.log('ðŸŽ¯ Step 3: Applying CDR filtering for outbound calls...');
    const outboundData = outboundRecords.map(row => row.raw_data);
    // Apply CDR filtering if outbound calls exist
    if (outboundData.length > 0) {
      console.log(`ðŸ“Š Filtering ${cdrRecords.length} CDR records against ${outboundData.length} outbound calls...`);
      
      // Create an index of outbound calls by extension for faster lookup
      const outboundByExtension = new Map();
      outboundData.forEach(outboundCall => {
        const ext = normalizePhoneNumber(outboundCall.agent_ext || outboundCall.agent_extension);
        if (ext) {
          if (!outboundByExtension.has(ext)) {
            outboundByExtension.set(ext, []);
          }
          outboundByExtension.get(ext).push(outboundCall);
        }
      });
      
      console.log(`📊 Created extension index with ${outboundByExtension.size} unique extensions`);
      
      const filteredCDRs = cdrRecords.filter(cdr => cdrMatchesOutboundOptimized(cdr.raw_data, outboundByExtension));
      
      console.log(`📊 CDR filtering complete: ${filteredCDRs.length} matching records found`);
      
      // Process filtered CDRs
      for (const cdr of filteredCDRs) {
        // Extract follow_up_notes directly from the record
        const notes = cdr.raw_data.follow_up_notes || '';
        if (notes) {
          cdr.raw_data['Follow up notes'] = notes;
          const callId = cdr.raw_data.call_id || cdr.raw_data.callid;
          console.log(`ðŸ“ Found follow-up notes in CDR record ${callId}: "${notes.substring(0, 30)}${notes.length > 30 ? '...' : ''}"`);
        }
        
        // Process and sanitize CDR record for display
        const processedRecord = processCDRRecord(cdr.raw_data);
        
        // Replace raw_data with processed data completely
        cdr.raw_data = processedRecord;
        cdr.raw_data._recordType = 'cdr';
        cdr.raw_data.record_type = 'cdr'; // Also set for compatibility
        
        allRecords.push(cdr.raw_data);
      }
    } else {
      console.log('âš ï¸ No outbound calls found, skipping CDR records');
    }
    
    // Step 4: Process transfers to queue extensions and match with inbound calls
    console.log('ðŸ”„ Step 4: Processing transfers to queue extensions...');

    // Extract outbound calls with ANY transfers (both queue and agent extensions)
    const outboundCallsWithTransfers = allRecords.filter(r =>
      r._recordType === 'outbound' &&
      r.transfer_event &&
      (r.transfer_extension || r.transfer_queue_extension)
    );
    
    console.log(`Found ${outboundCallsWithTransfers.length} outbound calls with transfers (both queue and agent extensions)`);
    
    // Process transfers to queue extensions and match with inbound calls
    if (outboundCallsWithTransfers.length > 0) {
      console.log('Matching outbound calls with transfers to inbound calls...');
      
      // Get inbound calls for matching
      const inboundCallsForMatching = allRecords.filter(record => record._recordType === 'inbound');
      
      console.log(`ðŸ“Š INBOUND ANALYSIS: Found ${inboundCallsForMatching.length} inbound calls for matching`);
      
      // Enhanced logging for inbound calls with callee_id analysis
      const calleeIdCounts = {};
      inboundCallsForMatching.forEach(call => {
        const calleeId = call.callee_id_number || 
          (call.raw_data && typeof call.raw_data === 'object' ? call.raw_data.callee_id_number : null);
        if (calleeId) {
          calleeIdCounts[calleeId] = (calleeIdCounts[calleeId] || 0) + 1;
        }
      });
      
      console.log(`ðŸ“Š CALLEE_ID DISTRIBUTION: ${Object.keys(calleeIdCounts).length} unique callee_id_numbers found`);
      Object.entries(calleeIdCounts).forEach(([calleeId, count]) => {
        console.log(`   ${calleeId}: ${count} calls`);
      });
      
      // Special check for 7014 (maps to queue extension 8001)
      const calls7014 = inboundCallsForMatching.filter(call => {
        const calleeId = call.callee_id_number || 
          (call.raw_data && typeof call.raw_data === 'object' ? call.raw_data.callee_id_number : null);
        return calleeId === '7014';
      });
      
      if (calls7014.length > 0) {
        console.log(`ðŸ’¡ SPECIAL CASE: Found ${calls7014.length} inbound calls with callee_id=7014 (for queue extension 8001)`);
        calls7014.forEach((call, idx) => {
          const callTime = call.called_time || call.timestamp;
          const callTimeMs = callTime < 10000000000 ? callTime * 1000 : callTime;
          console.log(`ðŸ’¡ 7014 Call ${idx + 1}: ID ${call.callid || call.call_id}, Time: ${new Date(callTimeMs).toISOString()}`);
          
          // Check for agent information
          if (Array.isArray(call.agent_history) && call.agent_history.length > 0) {
            const firstAgent = call.agent_history[0];
            const agentExt = firstAgent.ext || 'N/A';
            const agentName = `${firstAgent.first_name || ''} ${firstAgent.last_name || ''}`.trim() || 'Unknown';
            console.log(`ðŸ’¡ 7014 Agent: ${agentName} (Extension: ${agentExt})`);
          } else {
            const altExt = call.agent_answered_ext || call.agent_ext || call.extension || 'N/A';
            console.log(`ðŸ’¡ 7014 Agent: Alternative extension = ${altExt}`);
          }
        });
      } else {
        console.log(`âš ï¸ SPECIAL CASE: No inbound calls found with callee_id=7014 (expected for queue extension 8001)`);
      }
      
      // Debug: Log sample inbound call structure
      if (inboundCallsForMatching.length > 0) {
        console.log('Sample inbound call structure:', JSON.stringify(inboundCallsForMatching[0], null, 2).substring(0, 500) + '...');
      }
      
      // Debug: Log sample outbound call structure
      if (outboundCallsWithTransfers.length > 0) {
        console.log('Sample outbound call structure:', JSON.stringify(outboundCallsWithTransfers[0], null, 2).substring(0, 500) + '...');
      }
      
      // Process transfers and update allRecords
      
      // First, try to find matching inbound calls using the hold time + callee_id map method
      for (const outboundCall of outboundCallsWithTransfers) {
        // Find the last hold_start tied to a queue extension (8000â€“8999)
        if (Array.isArray(outboundCall.agent_history)) {
          const lastHoldStartEvent = findLastHoldStartForQueueExtension(outboundCall.agent_history);
          if (lastHoldStartEvent) {
            console.log(`Found hold_start event for call ${outboundCall.callid || outboundCall.call_id} at ${new Date(lastHoldStartEvent.last_attempt * 1000).toISOString()}`);
            
            // Try to find a matching inbound call using the hold time method
            const matchingCall = findMatchingInboundCallByHoldTime(
              outboundCall, lastHoldStartEvent, inboundCallsForMatching
            );
            
            if (matchingCall) {
              console.log(`📊 Found matching inbound call for outbound call ${outboundCall.callid || outboundCall.call_id}`);
              
              // Extract agent extension from the matching inbound call
              let agentExtension = null;
              
              // First try to get extension from agent_history
              if (Array.isArray(matchingCall.agent_history) && matchingCall.agent_history.length > 0) {
                for (const event of matchingCall.agent_history) {
                  if (event && event.ext) {
                    agentExtension = event.ext;
                    console.log(`Found agent extension ${agentExtension} from agent_history`);
                    break;
                  }
                }
              }
              
              // If we still don't have an agent extension, try other fields
              if (!agentExtension) {
                if (matchingCall.agent_answered_ext) {
                  agentExtension = matchingCall.agent_answered_ext;
                } else if (matchingCall.agent_ext) {
                  agentExtension = matchingCall.agent_ext;
                } else if (matchingCall.extension) {
                  agentExtension = matchingCall.extension;
                }
              }
              
              if (agentExtension) {
                const extStr = String(agentExtension);
                // Keep both machine and display-friendly keys
                outboundCall.transfer_extension = extStr;
                outboundCall['Transfer to Agent Extension'] = extStr;
                outboundCall.transfer_agent_extension = extStr; // alias for code/filters

                // Preserve the queue extension for clarity in UI
                const qExt = outboundCall.transfer_queue_extension ||
                             (isQueueExtension(outboundCall.transfer_extension) ? String(outboundCall.transfer_extension) : null);
                if (qExt) {
                  outboundCall['Transfer to Queue Extension'] = qExt;
                }
                
                outboundCall.transfer_source_call_id = matchingCall.callid || matchingCall.call_id;
                console.log(`Set transfer_extension=${extStr} (and preserved queue ext) for outbound ${outboundCall.callid || outboundCall.call_id}`);
              }
            }
          }
        }
      }
      
      // Then use the existing processTransfersToQueueExtensions function as a fallback
      const processedOutboundCalls = processTransfersToQueueExtensions(outboundCallsWithTransfers, inboundCallsForMatching);
      
      // Update the original records in allRecords with the processed ones
      processedOutboundCalls.forEach(processedCall => {
        const index = allRecords.findIndex(record => 
          (record.callid && record.callid === processedCall.callid) || 
          (record.call_id && record.call_id === processedCall.call_id)
        );
        
        if (index !== -1) {
          allRecords[index] = processedCall;
          console.log(`Updated record ${processedCall.callid || processedCall.call_id}:`);
          console.log(`  - Transfer Extension: ${processedCall.transfer_extension}`);
          console.log(`  - Transfer Queue Extension: ${processedCall.transfer_queue_extension}`);
        }
      });
      
      console.log(`📊 Processed ${processedOutboundCalls.length} outbound calls with transfers to queue extensions`);
    }
    
    // Process inbound calls with transfers to queue extensions
    console.log('ðŸ”„ Processing inbound calls for queue extension transfers...');
    
    const inboundCallsWithTransfers = allRecords.filter(r =>
      r._recordType === 'inbound' &&
      Array.isArray(r.agent_history) &&
      r.agent_history.some(e =>
        e && e.type === 'transfer' && e.event === 'transfer' &&
        e.ext
      )
    );
    
    console.log(`Found ${inboundCallsWithTransfers.length} inbound calls with transfers (both queue and agent extensions)`);
    
    if (inboundCallsWithTransfers.length > 0) {
      const allInboundCalls = allRecords.filter(record => record._recordType === 'inbound');
      
      console.log(`ðŸ“Š ENHANCED INBOUND ANALYSIS: Found ${inboundCallsWithTransfers.length} inbound calls with transfers (both queue and agent extensions)`);
      console.log(`ðŸ“Š INBOUND MATCHING POOL: Found ${allInboundCalls.length} inbound calls for matching`);
      
      // Enhanced processing for each inbound call with queue extension transfer
      for (const inboundCall of inboundCallsWithTransfers) {
        console.log(`ðŸ” ENHANCED INBOUND MATCHING: Processing inbound call ${inboundCall.callid || inboundCall.call_id}`);
        
        // Find transfer events with queue extensions in agent_history
        if (Array.isArray(inboundCall.agent_history)) {
          const transferEvents = inboundCall.agent_history
            .filter(e => e && e.type === 'attended' && e.event === 'transfer' && e.ext)
            .sort((a, b) => a.last_attempt - b.last_attempt);
          
          if (transferEvents.length > 0) {
            const lastTransferEvent = transferEvents[transferEvents.length - 1];
            const queueExtension = lastTransferEvent.ext;
            
            // Set transfer flags
            inboundCall.transfer_event = true;
            inboundCall.transfer_queue_extension = queueExtension;
            inboundCall.transfer_extension = queueExtension; // Will be updated with agent extension if match found
            
            console.log(`ðŸ”„ INBOUND Queue extension transfer detected: ${queueExtension}`);
            
            // Find the last hold_start event before this transfer
            const holdStartEvents = inboundCall.agent_history
              .filter(e => e && e.event === 'hold_start' && e.last_attempt && e.last_attempt <= lastTransferEvent.last_attempt)
              .sort((a, b) => b.last_attempt - a.last_attempt);
            
            if (holdStartEvents.length > 0) {
              const lastHoldStartEvent = holdStartEvents[0];
              
              console.log(`ðŸŽ¯ INBOUND HOLD START FOUND: Call ${inboundCall.callid || inboundCall.call_id} at ${new Date(lastHoldStartEvent.last_attempt * 1000).toISOString()}`);
              
              // Queue extension to callee_id mapping
              const queueToCalleeExtensionMap = {
                '8000': '7020', '8001': '7014', '8002': '7015', '8003': '7016', '8004': '7012',
                '8005': '7017', '8006': '7018', '8007': '7019', '8008': '7021', '8009': '7034',
                '8010': '7023', '8011': '7028', '8012': '7029', '8013': '7031', '8014': '7033',
                '8015': '7030', '8016': '7013', '8017': '7011', '8018': '7010', '8019': '7008'
              };
              
              const expectedCalleeId = queueToCalleeExtensionMap[queueExtension];
              if (expectedCalleeId) {
                console.log(`ðŸ” INBOUND MAPPING: Queue extension ${queueExtension} maps to callee_id_number ${expectedCalleeId}`);
                
                // Find matching inbound calls within 2-minute window
                const matchingCalls = allInboundCalls.filter(otherInboundCall => {
                  if (otherInboundCall.callid === inboundCall.callid || otherInboundCall.call_id === inboundCall.call_id) {
                    return false; // Skip self
                  }
                  
                  const calleeId = otherInboundCall.callee_id_number || 
                    (otherInboundCall.raw_data && typeof otherInboundCall.raw_data === 'object' ? otherInboundCall.raw_data.callee_id_number : null);
                  
                  if (calleeId !== expectedCalleeId) {
                    return false;
                  }
                  
                  const calledTime = otherInboundCall.called_time || otherInboundCall.timestamp;
                  if (!calledTime) {
                    return false;
                  }
                  
                  const holdStartMs = lastHoldStartEvent.last_attempt < 10000000000 ? 
                    lastHoldStartEvent.last_attempt * 1000 : lastHoldStartEvent.last_attempt;
                  const calledTimeMs = calledTime < 10000000000 ? calledTime * 1000 : calledTime;
                  
                  const timeDiff = Math.abs(calledTimeMs - holdStartMs);
                  const twoMinutesMs = 2 * 60 * 1000;
                  
                  return timeDiff <= twoMinutesMs;
                });
                
                if (matchingCalls.length > 0) {
                  const matchingCall = matchingCalls[0];
                  console.log(`📊 INBOUND ENHANCED MATCH FOUND: Inbound call ${matchingCall.callid || matchingCall.call_id} matches inbound call ${inboundCall.callid || inboundCall.call_id}`);
                  
                  // Extract agent extension
                  let agentExtension = null;
                  if (Array.isArray(matchingCall.agent_history) && matchingCall.agent_history.length > 0) {
                    for (const event of matchingCall.agent_history) {
                      if (event && event.ext) {
                        agentExtension = event.ext;
                        console.log(`ðŸ‘¤ INBOUND AGENT FOUND: ${agentExtension} from agent_history`);
                        break;
                      }
                    }
                  }
                  
                  if (!agentExtension) {
                    if (matchingCall.agent_answered_ext) {
                      agentExtension = matchingCall.agent_answered_ext;
                      console.log(`ðŸ‘¤ INBOUND AGENT FOUND: ${agentExtension} from agent_answered_ext`);
                    } else if (matchingCall.agent_ext) {
                      agentExtension = matchingCall.agent_ext;
                      console.log(`ðŸ‘¤ INBOUND AGENT FOUND: ${agentExtension} from agent_ext`);
                    } else if (matchingCall.extension) {
                      agentExtension = matchingCall.extension;
                      console.log(`ðŸ‘¤ INBOUND AGENT FOUND: ${agentExtension} from extension`);
                    }
                  }
                  
                  if (agentExtension) {
                    inboundCall.transfer_extension = agentExtension;
                    console.log(`📊 INBOUND ENHANCED UPDATE: Set transfer_extension to ${agentExtension} for call ${inboundCall.callid || inboundCall.call_id}`);
                  }
                } else {
                  console.log(`âš ï¸ INBOUND ENHANCED MATCHING: No matching inbound call found within time window for inbound call ${inboundCall.callid || inboundCall.call_id}`);
                  
                  // Fallback: Try to find any inbound call with matching callee_id (outside time window)
                  const fallbackCalls = allInboundCalls.filter(otherInboundCall => {
                    if (otherInboundCall.callid === inboundCall.callid || otherInboundCall.call_id === inboundCall.call_id) {
                      return false;
                    }
                    const calleeId = otherInboundCall.callee_id_number || 
                      (otherInboundCall.raw_data && typeof otherInboundCall.raw_data === 'object' ? otherInboundCall.raw_data.callee_id_number : null);
                    return calleeId === expectedCalleeId;
                  });
                  
                  if (fallbackCalls.length > 0) {
                    const fallbackCall = fallbackCalls[0];
                    console.log(`ðŸ”„ INBOUND ENHANCED FALLBACK MATCH: Using closest inbound call ${fallbackCall.callid || fallbackCall.call_id}`);
                    
                    let agentExtension = null;
                    if (Array.isArray(fallbackCall.agent_history) && fallbackCall.agent_history.length > 0) {
                      for (const event of fallbackCall.agent_history) {
                        if (event && event.ext) {
                          agentExtension = event.ext;
                          console.log(`ðŸ‘¤ INBOUND FALLBACK AGENT: ${agentExtension} from agent_history`);
                          break;
                        }
                      }
                    }
                    
                    if (!agentExtension && fallbackCall.agent_answered_ext) {
                      agentExtension = fallbackCall.agent_answered_ext;
                      console.log(`📊 INBOUND FALLBACK AGENT: ${agentExtension} from agent_answered_ext`);
                    }
                    
                    if (agentExtension) {
                      inboundCall.transfer_extension = agentExtension;
                      console.log(`📊 INBOUND ENHANCED FALLBACK SET: transfer_extension=${agentExtension} for call ${inboundCall.callid || inboundCall.call_id}`);
                    }
                  }
                }
              }
            } else {
              console.log(`📊 INBOUND HOLD START: No hold_start event found for call ${inboundCall.callid || inboundCall.call_id}`);
            }
          }
        }
      }
      
      console.log(`📊 Enhanced processed ${inboundCallsWithTransfers.length} inbound calls with transfers to queue extensions`);
    }
    
    // Process campaign calls with transfers to queue extensions
    console.log('📊 Processing campaign calls for queue extension transfers...');
    
    const campaignCallsWithTransfers = allRecords.filter(r =>
      r._recordType === 'campaign' &&
      Array.isArray(r.lead_history) &&
      r.lead_history.some(e =>
        e && e.type === 'Transfer' && e.ext
      )
    );
    
    console.log(`Found ${campaignCallsWithTransfers.length} campaign calls with transfers (both queue and agent extensions)`);
    
    if (campaignCallsWithTransfers.length > 0) {
      const inboundCallsForCampaignMatching = allRecords.filter(record => record._recordType === 'inbound');
      
      console.log(`ðŸ“Š ENHANCED CAMPAIGN ANALYSIS: Found ${campaignCallsWithTransfers.length} campaign calls with transfers (both queue and agent extensions)`);
      console.log(`ðŸ“Š CAMPAIGN MATCHING POOL: Found ${inboundCallsForCampaignMatching.length} inbound calls for matching`);
      
      // Enhanced processing for each campaign call with queue extension transfer
      for (const campaignCall of campaignCallsWithTransfers) {
        console.log(`ðŸ” ENHANCED CAMPAIGN MATCHING: Processing campaign call ${campaignCall.callid || campaignCall.call_id}`);
        
        // Find transfer events with queue extensions in lead_history
        if (Array.isArray(campaignCall.lead_history)) {
          const transferEvents = campaignCall.lead_history
            .filter(e => e && e.type === 'Transfer' && e.ext)
            .sort((a, b) => a.last_attempt - b.last_attempt);
          
          if (transferEvents.length > 0) {
            const lastTransferEvent = transferEvents[transferEvents.length - 1];
            const queueExtension = lastTransferEvent.ext;
            
            // Set transfer flags
            campaignCall.transfer_event = true;
            campaignCall.transfer_queue_extension = queueExtension;
            campaignCall.transfer_extension = queueExtension; // Will be updated with agent extension if match found
            
            console.log(`ðŸ”„ CAMPAIGN Queue extension transfer detected: ${queueExtension}`);
            
            // Find the last lead_answer event before this transfer (campaign-specific logic)
            const leadAnswerEvents = campaignCall.lead_history
              .filter(e => e && e.type === 'lead_answer' && e.last_attempt && e.last_attempt <= lastTransferEvent.last_attempt)
              .sort((a, b) => b.last_attempt - a.last_attempt);
            
            if (leadAnswerEvents.length > 0) {
              const lastLeadAnswerEvent = leadAnswerEvents[0];
              
              console.log(`ðŸŽ¯ CAMPAIGN LEAD ANSWER FOUND: Call ${campaignCall.callid || campaignCall.call_id} at ${new Date(lastLeadAnswerEvent.last_attempt * 1000).toISOString()}`);
              
              // Queue extension to callee_id mapping
              const queueToCalleeExtensionMap = {
                '8000': '7020', '8001': '7014', '8002': '7015', '8003': '7016', '8004': '7012',
                '8005': '7017', '8006': '7018', '8007': '7019', '8008': '7021', '8009': '7034',
                '8010': '7023', '8011': '7028', '8012': '7029', '8013': '7031', '8014': '7033',
                '8015': '7030', '8016': '7013', '8017': '7011', '8018': '7010', '8019': '7008'
              };
              
              const expectedCalleeId = queueToCalleeExtensionMap[queueExtension];
              if (expectedCalleeId) {
                console.log(`ðŸ” CAMPAIGN MAPPING: Queue extension ${queueExtension} maps to callee_id_number ${expectedCalleeId}`);
                
                // Find matching inbound calls within 2-minute window of lead_answer time
                const matchingCalls = inboundCallsForCampaignMatching.filter(inboundCall => {
                  const calleeId = inboundCall.callee_id_number || 
                    (inboundCall.raw_data && typeof inboundCall.raw_data === 'object' ? inboundCall.raw_data.callee_id_number : null);
                  
                  if (calleeId !== expectedCalleeId) {
                    return false;
                  }
                  
                  const calledTime = inboundCall.called_time || inboundCall.timestamp;
                  if (!calledTime) {
                    return false;
                  }
                  
                  const leadAnswerMs = lastLeadAnswerEvent.last_attempt < 10000000000 ? 
                    lastLeadAnswerEvent.last_attempt * 1000 : lastLeadAnswerEvent.last_attempt;
                  const calledTimeMs = calledTime < 10000000000 ? calledTime * 1000 : calledTime;
                  
                  const timeDiff = Math.abs(calledTimeMs - leadAnswerMs);
                  const twoMinutesMs = 2 * 60 * 1000;
                  
                  return timeDiff <= twoMinutesMs;
                });
                
                if (matchingCalls.length > 0) {
                  const matchingCall = matchingCalls[0];
                  console.log(`📊 CAMPAIGN ENHANCED MATCH FOUND: Inbound call ${matchingCall.callid || matchingCall.call_id} matches campaign call ${campaignCall.callid || campaignCall.call_id}`);
                  
                  // Extract agent extension
                  let agentExtension = null;
                  if (Array.isArray(matchingCall.agent_history) && matchingCall.agent_history.length > 0) {
                    for (const event of matchingCall.agent_history) {
                      if (event && event.ext) {
                        agentExtension = event.ext;
                        console.log(`ðŸ‘¤ CAMPAIGN AGENT FOUND: ${agentExtension} from agent_history`);
                        break;
                      }
                    }
                  }
                  
                  if (!agentExtension) {
                    if (matchingCall.agent_answered_ext) {
                      agentExtension = matchingCall.agent_answered_ext;
                      console.log(`ðŸ‘¤ CAMPAIGN AGENT FOUND: ${agentExtension} from agent_answered_ext`);
                    } else if (matchingCall.agent_ext) {
                      agentExtension = matchingCall.agent_ext;
                      console.log(`ðŸ‘¤ CAMPAIGN AGENT FOUND: ${agentExtension} from agent_ext`);
                    } else if (matchingCall.extension) {
                      agentExtension = matchingCall.extension;
                      console.log(`ðŸ‘¤ CAMPAIGN AGENT FOUND: ${agentExtension} from extension`);
                    }
                  }
                  
                  if (agentExtension) {
                    campaignCall.transfer_extension = agentExtension;
                    console.log(`📊 CAMPAIGN ENHANCED UPDATE: Set transfer_extension to ${agentExtension} for call ${campaignCall.callid || campaignCall.call_id}`);
                    console.log(`📊 CAMPAIGN TRANSFER FLOW: lead_answer -> Queue ${queueExtension} -> Callee ID ${expectedCalleeId} -> Agent Extension ${agentExtension}`);
                  }
                } else {
                  console.log(`📊 CAMPAIGN ENHANCED MATCHING: No matching inbound call found within time window for campaign call ${campaignCall.callid || campaignCall.call_id}`);
                  
                  // Fallback: Try to find any inbound call with matching callee_id (outside time window)
                  const fallbackCalls = inboundCallsForCampaignMatching.filter(inboundCall => {
                    const calleeId = inboundCall.callee_id_number || 
                      (inboundCall.raw_data && typeof inboundCall.raw_data === 'object' ? inboundCall.raw_data.callee_id_number : null);
                    return calleeId === expectedCalleeId;
                  });
                  
                  if (fallbackCalls.length > 0) {
                    // Sort by time difference to get the closest match (even if outside window)
                    fallbackCalls.sort((a, b) => {
                      const aTime = a.called_time || a.timestamp;
                      const bTime = b.called_time || b.timestamp;
                      const aTimeDiff = Math.abs((aTime < 10000000000 ? aTime * 1000 : aTime) - 
                        (lastLeadAnswerEvent.last_attempt < 10000000000 ? lastLeadAnswerEvent.last_attempt * 1000 : lastLeadAnswerEvent.last_attempt));
                      const bTimeDiff = Math.abs((bTime < 10000000000 ? bTime * 1000 : bTime) - 
                        (lastLeadAnswerEvent.last_attempt < 10000000000 ? lastLeadAnswerEvent.last_attempt * 1000 : lastLeadAnswerEvent.last_attempt));
                      return aTimeDiff - bTimeDiff;
                    });
                    
                    const fallbackCall = fallbackCalls[0];
                    console.log(`ðŸ”„ CAMPAIGN ENHANCED FALLBACK MATCH: Using closest inbound call ${fallbackCall.callid || fallbackCall.call_id}`);
                    
                    let agentExtension = null;
                    if (Array.isArray(fallbackCall.agent_history) && fallbackCall.agent_history.length > 0) {
                      for (const event of fallbackCall.agent_history) {
                        if (event && event.ext) {
                          agentExtension = event.ext;
                          console.log(`ðŸ‘¤ CAMPAIGN FALLBACK AGENT: ${agentExtension} from agent_history`);
                          break;
                        }
                      }
                    }
                    
                    if (!agentExtension && fallbackCall.agent_answered_ext) {
                      agentExtension = fallbackCall.agent_answered_ext;
                      console.log(`ðŸ‘¤ CAMPAIGN FALLBACK AGENT: ${agentExtension} from agent_answered_ext`);
                    }
                    
                    if (agentExtension) {
                      campaignCall.transfer_extension = agentExtension;
                      console.log(`📊 CAMPAIGN ENHANCED FALLBACK SET: transfer_extension=${agentExtension} for call ${campaignCall.callid || campaignCall.call_id}`);
                      console.log(`📊 CAMPAIGN FALLBACK FLOW: lead_answer -> Queue ${queueExtension} -> Callee ID ${expectedCalleeId} -> Agent Extension ${agentExtension}`);
                    }
                  } else {
                    console.log(`âŒ CAMPAIGN ENHANCED FALLBACK: No inbound calls found with callee_id ${expectedCalleeId} at all`);
                  }
                }
              }
            } else {
              console.log(`âš ï¸ CAMPAIGN LEAD ANSWER: No lead_answer event found before transfer for call ${campaignCall.callid || campaignCall.call_id}`);
            }
          }
        }
      }
      
      console.log(`📊 Enhanced processed ${campaignCallsWithTransfers.length} campaign calls with transfers to queue extensions`);
    }
    
    // Step 5: Sort all records by time
    console.log('â° Step 5: Sorting all records by timestamp...');
    
    // Sort all records by timestamp
    allRecords.sort((a, b) => {
      const timeA = getRecordTimestamp(a);
      const timeB = getRecordTimestamp(b);
      return timeA - timeB;
    });
    
    const cdrCount = allRecords.filter(r => r.record_type === 'CDR').length;
    const recordsWithFollowUp = allRecords.filter(r => r['Follow up notes'] && r['Follow up notes'].trim() !== '').length;
    
    console.log(`📊 Unified report created: ${allRecords.length} total records`);
    console.log(`   - Campaigns: ${campaignRecords.length}`);
    console.log(`   - Inbound: ${inboundRecords.length}`);
    console.log(`   - Outbound: ${outboundRecords.length}`);
    console.log(`   - Records with follow-up notes: ${recordsWithFollowUp}`);
    
    return {
      rows: allRecords,
      summary: {
        total: allRecords.length,
        campaigns: campaignRecords.length,
        inbound: inboundRecords.length,
        outbound: outboundRecords.length,
        cdr: cdrCount,
        recordsWithFollowUp
      }
    };
  } catch (error) {
    console.error('âŒ Error creating unified report from database:', error);
    console.error('âŒ Error in unified report fetch:', error);
    throw error;
  }
}

/**
 * Get timestamp from a record for sorting
 * @param {Object} record - Record to extract timestamp from
 * @returns {number} - Timestamp in milliseconds
 */
function getRecordTimestamp(record) {
  // Priority order of timestamp fields
  const timestampFields = [
    'event_timestamp', 'called_time', 'answered_time', 
    'hangup_time', 'timestamp', 'datetime'
  ];
  
  for (const field of timestampFields) {
    if (record[field] != null && record[field] !== '') {
      const ts = Number(record[field]);
      if (!isNaN(ts)) {
        // Handle Gregorian timestamp conversion for all records
        // Gregorian timestamps are typically > 60000000000 (around year 1900+)
        if (ts > 60000000000) {
          // Gregorian timestamp: subtract 62167219200 seconds (offset from 0001-01-01 to 1970-01-01)
          const unixSeconds = ts - 62167219200;
          return unixSeconds * 1000; // Convert to milliseconds
        }
        
        // Convert to milliseconds if needed (regular Unix timestamps)
        return ts < 10_000_000_000 ? ts * 1000 : ts;
      }
      // Try parsing as date string
      const date = new Date(record[field]);
      if (!isNaN(date.getTime())) {
        return date.getTime();
      }
    }
  }
  
  // Return 0 if no valid timestamp found
  return 0;
}

/**
 * Process record data for unified report
 * @param {Object} record - Record to process
 * @param {string} reportType - Type of report (campaignsActivity, queueCalls, queueOutboundCalls, cdrs)
 * @param {Array} cdrAllRecords - Optional array of cdrs_all records for follow-up notes extraction
 */
export function processRecordData(record, reportType, cdrAllRecords = []) {
  // Helper functions for data processing
  function isoToLocal(dateStr) {
    return new Date(dateStr).toLocaleString('en-GB', { timeZone: 'Asia/Dubai' });
  }
  
  // Helper to wrap arbitrary HTML in an eye button that opens a modal
  function createEyeBtn(innerHtml) {
    const id = 'popup_' + Math.random().toString(36).slice(2, 9);
    return `<button class="button is-small is-rounded eye-btn" data-target="${id}" title="View">&#128065;</button>` +
           `<div id="${id}" class="popup-content" style="display:none">${innerHtml}</div>`;
  }
  function formatTimestamp(ts) {
    if (ts == null || ts === '' || ts === undefined) return '';
    
    try {
      // Handle different timestamp formats
      let timestamp = ts;
      if (typeof ts === 'string') {
        timestamp = parseFloat(ts);
        // Check if parsing resulted in NaN
        if (isNaN(timestamp)) return '';
      }
      
      // Validate timestamp is a number
      if (typeof timestamp !== 'number' || isNaN(timestamp)) return '';
      
      // Handle Gregorian timestamps (convert to Unix timestamp)
      if (timestamp > 60000000000) {
        const unixSeconds = timestamp - 62167219200;
        timestamp = unixSeconds * 1000; // Convert to milliseconds
      } else if (timestamp < 10_000_000_000) {
        // Convert Unix seconds to milliseconds
        timestamp = timestamp * 1000;
      }
      
      // Validate timestamp range (reasonable date range)
      if (timestamp < 0 || timestamp > 4102444800000) { // Year 2100
        return '';
      }
      
      // Create date and format to local time
      const date = new Date(timestamp);
      if (isNaN(date.getTime())) return '';
      
      return date.toLocaleString('en-GB', { 
        timeZone: 'Asia/Dubai',
        day: '2-digit',
        month: '2-digit', 
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
      });
    } catch (error) {
      console.warn('Error formatting timestamp:', ts, error);
      return '';
    }
  }
  function formatDuration(sec) {
    const total = parseInt(sec, 10);
    if (Number.isNaN(total)) return '';
    const h = Math.floor(total / 3600).toString().padStart(2, '0');
    const m = Math.floor((total % 3600) / 60).toString().padStart(2, '0');
    const s = (total % 60).toString().padStart(2, '0');
    return `${h}:${m}:${s}`;
  }
  function historyToHtml(hist){ 
    if(!Array.isArray(hist)||!hist.length) return ''; 
    const sorted = [...hist].sort((a,b)=>(a.last_attempt??0)-(b.last_attempt??0));
    const COLS = [
      { key: 'last_attempt', label: 'Last Attempt' },
      { key: 'name', label: 'Name' },
      { key: 'ext', label: 'Extension' },
      { key: 'type', label: 'Type' },
      { key: 'event', label: 'Event' },
      { key: 'connected', label: 'Connected' },
      { key: 'queue_name', label: 'Queue Name' }
    ];
    const thead = `<thead><tr>${COLS.map(c => `<th>${c.label}</th>`).join('')}</tr></thead>`;
    const rows = sorted.map(h => {
      const cells = COLS.map(c => {
        let val = '';
        if (c.key === 'name') {
          val = `${h.first_name || ''} ${h.last_name || ''}`.trim();
        } else if (c.key === 'last_attempt') {
          if (h.last_attempt) {
            const ms = h.last_attempt > 10_000_000_000 ? h.last_attempt : h.last_attempt * 1000;
            val = new Date(ms).toLocaleString('en-GB', { timeZone: 'Asia/Dubai' });
          }
        } else if (c.key === 'connected') {
          val = h.connected ? 'Yes' : 'No';
        } else {
          val = h[c.key] ?? '';
        }
        return `<td>${val}</td>`;
      }).join('');
      return `<tr>${cells}</tr>`;
    }).join('');
    const tableHtml = `<table class="history-table">${thead}<tbody>${rows}</tbody></table>`;
    return createEyeBtn(tableHtml);
  }
  function queueHistoryToHtml(hist){ 
    if(!Array.isArray(hist)||!hist.length) return ''; 
    const thead = '<thead><tr><th>Date</th><th>Queue Name</th></tr></thead>';
    const rows = hist.map(h => {
      let date = '';
      if (h.ts) {
        const ms = h.ts > 10_000_000_000 ? h.ts : h.ts * 1000;
        date = new Date(ms).toLocaleString('en-GB', { timeZone: 'Asia/Dubai' });
      }
      const q = h.queue_name ?? '';
      return `<tr><td>${date}</td><td>${q}</td></tr>`;
    }).join('');
    const tableHtml = `<table class="history-table">${thead}<tbody>${rows}</tbody></table>`;
    return createEyeBtn(tableHtml);
  }
  function leadHistoryToHtml(hist){ 
    if(!Array.isArray(hist)||!hist.length) return ''; 
    const sorted = [...hist].sort((a,b)=>(a.last_attempt??0)-(b.last_attempt??0));
    const thead = '<thead><tr><th>Last Attempt</th><th>First Name</th><th>Last Name</th><th>Extension/Number</th><th>Event</th><th>Hangup Cause</th></tr></thead>';
    const rows = sorted.map(h => {
      let last = '';
      if (h.last_attempt) {
        const ms = h.last_attempt > 10_000_000_000 ? h.last_attempt : h.last_attempt * 1000;
        last = new Date(ms).toLocaleString('en-GB', { timeZone: 'Asia/Dubai' });
      }
      const fn = h.agent?.first_name ?? '';
      const ln = h.agent?.last_name ?? '';
      const ext = h.agent?.ext ?? '';
      const evt = h.type || h.event || '';
      const cause = h.hangup_cause || '';
      return `<tr><td>${last}</td><td>${fn}</td><td>${ln}</td><td>${ext}</td><td>${evt}</td><td>${cause}</td></tr>`;
    }).join('');
    const tableHtml = `<table class="history-table">${thead}<tbody>${rows}</tbody></table>`;
    return createEyeBtn(tableHtml);
  }
  function extractCampaignAgentName(agentHistory){ if(!agentHistory) return ''; try{ if(typeof agentHistory==='string') agentHistory=JSON.parse(agentHistory); }catch{return'';} if(!Array.isArray(agentHistory)) return ''; const ent=agentHistory.find(e=>e.event==='agent_answer'); if(!ent) return ''; return `${ent.first_name??''} ${ent.last_name??''}`.trim(); }
  function hasAgentHangupEvent(hist){
    if(!hist) return 'No';
    try{ if(typeof hist==='string') hist=JSON.parse(hist);}catch{return 'No';}
    if(Array.isArray(hist)){
      return hist.some(e=>e.event==='agent_hangup') ? 'Yes' : 'No';
    }
    return 'No';
  }

  // Add Extension column logic
  let extension = '';
  if (reportType === 'campaignsActivity') {
    extension = record.agent_extension || '';
  } else if (reportType === 'queueCalls' || reportType === 'queueOutboundCalls') {
    if (Array.isArray(record.agent_history) && record.agent_history.length > 0) {
      // For inbound calls, find the agent with "agent_enter" event
      if (reportType === 'queueCalls') {
        const agentEnterEntry = record.agent_history.find(entry => entry.event === 'agent_enter');
        extension = agentEnterEntry ? agentEnterEntry.ext || '' : '';
      } else {
        // For outbound calls, keep existing logic (first agent)
        const firstAgentHistory = record.agent_history[0];
        extension = firstAgentHistory.ext || '';
      }
    }
  } else if (reportType === 'cdrs') {
    extension = '';
  }
  record.Extension = extension;

  // Add Country column logic
  let country = '';
  if (reportType === 'campaignsActivity') {
    country = extractCountryFromPhoneNumber(record.lead_number) || '';
  } else if (reportType === 'queueCalls') {
    country = extractCountryFromPhoneNumber(record.caller_id_number) || '';
  } else if (reportType === 'queueOutboundCalls') {
    const phoneNumber = record.to || record.destination || record.callee_id_number;
    country = extractCountryFromPhoneNumber(phoneNumber) || '';
  } else if (reportType === 'cdrs') {
    country = extractCountryFromPhoneNumber(record.caller_id_number) || extractCountryFromPhoneNumber(record.callee_id_number) || '';
  }
  record.Country = country;

  // Add CSAT column logic
  const csat = extractCSATFromAgentHistory(record.agent_history);
  record.CSAT = csat;

  // Add Agent Hangup column logic
  let agentHangup = 'No';
  if (reportType === 'queueCalls' || reportType === 'queueOutboundCalls') {
    if (Array.isArray(record.agent_history) && record.agent_history.length > 0) {
      const hasAgentHangupEvent = record.agent_history.some(entry => entry.event === 'agent_hangup');
      agentHangup = hasAgentHangupEvent ? 'Yes' : 'No';
    }
  } else if (reportType === 'campaignsActivity') {
    if (Array.isArray(record.agent_history) && record.agent_history.length > 0) {
      let agentHangupIndex = -1;
      let leadHangupIndex = -1;
      
      record.agent_history.forEach((entry, index) => {
        if (entry.event === 'agent_hangup') {
          agentHangupIndex = index;
        } else if (entry.event === 'lead_hangup') {
          leadHangupIndex = index;
        }
      });
      
      if (agentHangupIndex !== -1 && (leadHangupIndex === -1 || agentHangupIndex < leadHangupIndex)) {
        agentHangup = 'Yes';
      }
    }
  }
  record['Agent Hangup'] = agentHangup;

  // Additional enrichment for CDR records
  if (reportType === 'cdrs') {
    // Handle basic CDR records (without fonoUC enrichment)
    record.Extension = record.extension ?? record.Extension ?? '';
    record['Agent name'] = extractCampaignAgentName(record.agent_history) || (record.agent_name ?? '');
    record['Agent Disposition'] = record.agent_disposition ?? '';
    record['Called Time'] = record.timestamp ? formatTimestamp(record.timestamp) : '';
    record['Answered time'] = record.answered_time ? formatTimestamp(record.answered_time) : '';
    record['Hangup time'] = record.hangup_time ? formatTimestamp(record.hangup_time) : '';
    record['Wait Duration'] = record.wait_duration ? formatDuration(record.wait_duration) : '';
    
    // Calculate hold duration from agent_history events
    if (Array.isArray(record.agent_history) && record.agent_history.length > 0) {
      const holdDurationSeconds = calculateHoldDurationFromHistory(record.agent_history, record.hangup_time);
      record.hold_duration = holdDurationSeconds;
      record['Hold Duration'] = holdDurationSeconds ? formatDuration(parseFloat(holdDurationSeconds)) : '';
    } else {
      record['Hold Duration'] = record.hold_duration ? formatDuration(record.hold_duration) : '';
    }
    record['Agent Hangup'] = hasAgentHangupEvent(record.agent_history) ? 'Yes' : 'No';
    record['Status'] = record.hangup_cause ?? record.status ?? '';
    record['Campaign Type'] = record.campaign_type ?? '';
    record['Abandoned'] = record.abandoned ?? '';
    record['Agent History'] = `${historyToHtml(record.agent_history ?? [])}${leadHistoryToHtml(record.lead_history ?? [])}`;
    record['Queue History'] = Array.isArray(record.queue_history) ? queueHistoryToHtml(record.queue_history) : '';
    record.CSAT = record.csat ?? '';
    
    // Extract follow-up notes for CDR records
    let followUpNotes = '';
    // Extract follow_up_notes directly from the response
    followUpNotes = record.follow_up_notes || '';
    record['Follow up notes'] = followUpNotes;
    
    // Set queue/campaign name for CDRs (usually empty for basic CDRs)
    record['Queue / Campaign Name'] = record.queue_name || record.campaign_name || '';

  }

  // Derive durations for queue reports if missing
  if (reportType === 'queueCalls' || reportType === 'queueOutboundCalls') {
    // Talked duration
    if (!record.talked_duration && record.hangup_time && record.answered_time) {
      record.talked_duration = record.hangup_time - record.answered_time;
    }
    // Wait / queue duration
    if (!record.wait_duration && record.called_time) {
      if (record.answered_time) {
        record.wait_duration = record.answered_time - record.called_time;
      } else if (record.hangup_time) {
        record.wait_duration = record.hangup_time - record.called_time;
      }
    }
  }

  // Handle abandoned flag for inbound calls
  if (reportType === 'queueCalls') {
    // Only calculate abandoned if it's not already provided by the API
    if (record.abandoned === undefined) {
      const hist = record.agent_history;
      const histMissing =
        hist == null ||
        (Array.isArray(hist) && hist.length === 0) ||
        (Array.isArray(hist) && hist.every(h => h && Object.keys(h).length === 0));

      let histNoAnswer = false;
      if (Array.isArray(hist) && hist.length > 0) {
        histNoAnswer = hist.every(h => !h?.answered_time && !h?.agent_action?.includes("transfer"));
      }

      const isAbandoned = histMissing || !record.answered_time || histNoAnswer;
      record.abandoned = isAbandoned ? "YES" : "NO";
    }
    // Preserve case consistency with the rest of the application
    if (typeof record.abandoned === 'string') {
      record.abandoned = record.abandoned.toUpperCase() === 'YES' || record.abandoned.toUpperCase() === 'TRUE' ? "YES" : "NO";
    }
  }

  // Map all required fields for inbound and outbound records
  if (reportType === 'queueCalls' || reportType === 'queueOutboundCalls') {
    // Detect transfer events in agent_history
    if (Array.isArray(record.agent_history)) {
      const callType = reportType === 'queueCalls' ? 'inbound' : 'outbound';
      const transferInfo = detectTransferEvents(record.agent_history, callType, record);
      record.transfer_event = transferInfo.transfer_event;
        record.transfer_extension = transferInfo.transfer_extension;
        record.transfer_queue_extension = transferInfo.transfer_queue_extension;
        record.transfer_type = transferInfo.transfer_type;
      
      if (transferInfo.transfer_event) {
        console.log(`ðŸ”„ ${callType.toUpperCase()} TRANSFER DETECTED: Call ID ${record.callid || record.call_id}, Extension: ${transferInfo.transfer_extension}`);
      }
    }
    
    // Extract agent name from agent_history or direct field
    let agentName = record.agent_name || '';
    if (!agentName && Array.isArray(record.agent_history) && record.agent_history.length > 0) {
      // For inbound calls, find the agent with "agent_enter" event
      if (reportType === 'queueCalls') {
        const agentEnterEntry = record.agent_history.find(entry => entry.event === 'agent_enter');
        if (agentEnterEntry) {
          agentName = `${agentEnterEntry.first_name || ''} ${agentEnterEntry.last_name || ''}`.trim();
        }
      } else {
        // For outbound calls, keep existing logic (first agent)
        const firstAgentHistory = record.agent_history[0];
        agentName = `${firstAgentHistory.first_name || ''} ${firstAgentHistory.last_name || ''}`.trim();
      }
    }
    
    // Map all frontend fields
    record['Agent name'] = agentName;
    
    // Set Queue / Campaign Name directly from the API response fields
    record['Queue / Campaign Name'] = record.queue_name || record.campaign_name || '';
    
    record['Called Time'] = record.called_time ? formatTimestamp(record.called_time) : '';
    record['Answered time'] = record.answered_time ? formatTimestamp(record.answered_time) : '';
    record['Hangup time'] = record.hangup_time ? formatTimestamp(record.hangup_time) : '';
    record['Wait Duration'] = record.wait_duration ? formatDuration(record.wait_duration) : '';
    record['Talk Duration'] = record.talked_duration ? formatDuration(record.talked_duration) : '';
    
    // Calculate hold duration from agent_history events
    if (Array.isArray(record.agent_history) && record.agent_history.length > 0) {
      const holdDurationSeconds = calculateHoldDurationFromHistory(record.agent_history, record.hangup_time);
      record.hold_duration = holdDurationSeconds;
      record['Hold Duration'] = holdDurationSeconds ? formatDuration(parseFloat(holdDurationSeconds)) : '';
    } else {
      record['Hold Duration'] = record.hold_duration ? formatDuration(record.hold_duration) : '';
    }
    record['Agent Disposition'] = record.agent_disposition || '';
    
    // Handle subdispositions (including third level)
    if (record.agent_subdisposition) {
      if (typeof record.agent_subdisposition === 'object' && record.agent_subdisposition.name) {
        record['Sub_disp_1'] = record.agent_subdisposition.name;
        if (record.agent_subdisposition.subdisposition && record.agent_subdisposition.subdisposition.name) {
          record['Sub_disp_2'] = record.agent_subdisposition.subdisposition.name;
          // Extract third level subdisposition (e.g., "Not Resolved" / "Resolved")
          if (record.agent_subdisposition.subdisposition.subdisposition && record.agent_subdisposition.subdisposition.subdisposition.name) {
            record['Sub_disp_3'] = record.agent_subdisposition.subdisposition.subdisposition.name;
          } else {
            record['Sub_disp_3'] = '';
          }
        } else {
          record['Sub_disp_2'] = '';
          record['Sub_disp_3'] = '';
        }
      } else {
        record['Sub_disp_1'] = record.agent_subdisposition.toString();
        record['Sub_disp_2'] = '';
        record['Sub_disp_3'] = '';
      }
    } else {
      record['Sub_disp_1'] = '';
      record['Sub_disp_2'] = '';
      record['Sub_disp_3'] = '';
    }
    
    // Extract follow_up_notes directly from the response
    const followUpNotesQueue = record.follow_up_notes || '';
    record['Follow up notes'] = followUpNotesQueue;
    
    // Add System Disposition based on call type, directly from API response
    if (reportType === 'queueCalls') {
      // For inbound calls, API provides "ANSWER"
      record['System Disposition'] = record.disposition || '';
    } else if (reportType === 'queueOutboundCalls') {
      // For outbound calls, API provides "SUCCESS"
      record['System Disposition'] = record.disposition || '';
    } else {
      // For other call types (like CDRs), use the disposition from the record
      record['System Disposition'] = record.disposition || '';
    }

    record['Status'] = record.status || '';
    record['Abandoned'] = record.abandoned || '';
    record['Agent History'] = historyToHtml(record.agent_history || []);
    record['Queue History'] = queueHistoryToHtml(record.queue_history || []);
    record['Recording'] = record.media_recording_id || record.recording_filename || '';
    record['Call ID'] = record.callid || record.call_id || '';
    
    // Phone number fields
    if (reportType === 'queueCalls') {
      record['Caller ID Number'] = record.caller_id_number || '';
      record['Caller ID / Lead Name'] = record.caller_id_name || '';
      record['Callee ID / Lead number'] = record.to || record.destination || '';
    } else if (reportType === 'queueOutboundCalls') {
      // For outbound calls: caller_id_number should be the agent extension, callee_id_number should be the customer number
      const agentExtension = record.agent_ext || 
        (Array.isArray(record.agent_history) && record.agent_history.length > 0 ? record.agent_history[0].ext : '') || '';
      const customerNumber = record.to || record.destination || record.caller_id_number || '';
      
      record['Caller ID Number'] = agentExtension;
      record['Caller ID / Lead Name'] = record.caller_id_name || '';
      record['Callee ID / Lead number'] = customerNumber;
    } else if (reportType === 'campaignsActivity') {
      // Debug: Log campaign record structure
      console.log('ðŸ” Campaign record keys:', Object.keys(record));


      // Campaign-specific field mappings with fallbacks
      const agentName = extractCampaignAgentName(record.agent_history) || record.agent_name || '';
      const campaignName = record.campaign_name || '';
      const calledTime = record.datetime ? formatTimestamp(record.datetime) : (record.timestamp ? formatTimestamp(record.timestamp) : '');
      const leadNumber = record.lead_number || '';
      const leadName = record.lead_name || `${record.lead_first_name || ''} ${record.lead_last_name || ''}`.trim();
      const answeredTime = record.lead_answer_time ? formatTimestamp(record.lead_answer_time) : '';
      const systemDisposition = record.disposition || '';
      const agentDisposition = record.agent_disposition || '';
      const status = record.status || '';
      const campaignType = record.campaign_type || '';
      const callId = record.call_id || '';
      const recording = record.media_recording_id || record.recording_filename || '';
      
      // Set the mapped fields
      record['Agent name'] = agentName;
      record['Extension'] = record.agent_extension || '';
      record['Queue / Campaign Name'] = campaignName;
      record['Called Time'] = calledTime;
      record['Caller ID Number'] = leadNumber;
      record['Caller ID / Lead Name'] = leadName;
      record['Callee ID / Lead number'] = leadNumber || record.lead_ticket_id || '';
      record['Answered time'] = answeredTime;
      record['Hangup time'] = record.hangup_time ? formatTimestamp(record.hangup_time) : '';
      record['Wait Duration'] = record.customer_wait_time_sla ? formatDuration(record.customer_wait_time_sla) : '';
      record['Talk Duration'] = record.agent_talk_time ? formatDuration(record.agent_talk_time) : '';
      
      // Calculate hold duration from agent_history events
      if (Array.isArray(record.agent_history) && record.agent_history.length > 0) {
        const holdDurationSeconds = calculateHoldDurationFromHistory(record.agent_history, record.hangup_time);
        record.hold_duration = holdDurationSeconds;
        record['Hold Duration'] = holdDurationSeconds ? formatDuration(parseFloat(holdDurationSeconds)) : '';
      } else {
        record['Hold Duration'] = record.hold_duration ? formatDuration(record.hold_duration) : '';
      }
      record['Agent Hangup'] = 'No'; // Default for campaigns
      record['Agent Disposition'] = agentDisposition;
      record['System Disposition'] = systemDisposition;
      // Handle subdisposition formatting properly (including third level)
      if (record.agent_subdisposition) {
        if (typeof record.agent_subdisposition === 'object' && record.agent_subdisposition.name) {
          record['Sub_disp_1'] = record.agent_subdisposition.name;
          record['Sub_disp_2'] = record.agent_subdisposition.subdisposition?.name ?? '';
          // Extract third level subdisposition (e.g., "Not Resolved" / "Resolved")
          record['Sub_disp_3'] = record.agent_subdisposition.subdisposition?.subdisposition?.name ?? '';
        } else if (typeof record.agent_subdisposition === 'string') {
          record['Sub_disp_1'] = record.agent_subdisposition;
          record['Sub_disp_2'] = '';
          record['Sub_disp_3'] = '';
        } else {
          record['Sub_disp_1'] = '';
          record['Sub_disp_2'] = '';
          record['Sub_disp_3'] = '';
        }
      } else {
        record['Sub_disp_1'] = '';
        record['Sub_disp_2'] = '';
        record['Sub_disp_3'] = '';
      }
      // Extract follow_up_notes directly from the response
      const followUpNotesCampaign = record.follow_up_notes || '';
      record['Follow up notes'] = followUpNotesCampaign;
      record['Abandoned'] = 'No'; // Default for campaigns
      record['Agent History'] = historyToHtml(record.agent_history || []);
      record['Queue History'] = historyToHtml(record.lead_history || []);
      record['Recording'] = recording;
      record['Country'] = extractCountryFromPhoneNumber(leadNumber) || '';
      record['Status'] = status;
      record['Campaign Type'] = campaignType;
      record['Call ID'] = callId;
      record.CSAT = record.csat ?? '';
      
      // Debug: Log mapped fields
      console.log('ðŸ” Campaign mapped fields:', {
        'Agent name': record['Agent name'],
        'Queue / Campaign Name': record['Queue / Campaign Name'],
        'Called Time': record['Called Time'],
        'Status': record['Status'],
        'Campaign Type': record['Campaign Type']
      });
    }
  }

  // Set the Type field based on report type
  if (reportType === 'queueCalls') {
    record['Type'] = 'Inbound';
    record._recordType = 'inbound';
  } else if (reportType === 'queueOutboundCalls') {
    record['Type'] = 'Outbound';
    record._recordType = 'outbound';
  } else if (reportType === 'campaignsActivity') {
    record['Type'] = 'Campaign';
    record._recordType = 'campaign';
  } else if (reportType === 'cdrs') {
    record['Type'] = 'CDR';
    record._recordType = 'cdr';
  } 
  // else if (reportType === 'cdrs_all') {
  //   record['Type'] = 'CDR';
  //   record._recordType = 'cdr_all';
  // }

  // Return the processed record
  return record;
}

/**
 * Fetch unified report from database (combines all report types)
 * @param {string} tenant - Tenant identifier
 * @param {Object} params - Query parameters (startDate, endDate, etc.)
 * @returns {Promise<Object>} - Combined report with all data sorted by time
 */
export async function fetchUnifiedReportFromDB(tenant, params = {}) {
  return await fetchUnifiedReport(tenant, params);
}

/**
 * Per-type report fetcher (used by server routes)
 * @param {string} report - Report type ('cdrs', 'cdrs_all', 'queueCalls', 'queueOutboundCalls', 'campaignsActivity')
 * @param {string} tenant - Tenant identifier
 * @param {Object} params - Query parameters (startDate, endDate, etc.)
 * @returns {Promise<Object>} - Report data for the specific type
 */
export async function fetchReport(report, tenant, params = {}) {
  return await fetchReportFromAPI(report, tenant, params);
}

/**
 * Legacy function that fetches data directly from API (DEPRECATED).
 * Use populateDatabase() to fetch and store data, then fetchReport() to retrieve from DB.
 * 
 * @param {string} report - One of: 'cdrs', 'cdrs_all', 'queueCalls', 'queueOutboundCalls', 'campaignsActivity'
 * @param {string} tenant - Tenant identifier
 * @param {Object} params - Query parameters (startDate, endDate, etc.)
 * @returns {Promise<Array>} - Array of all fetched records
 */
export async function fetchReportFromAPI(report, tenant, params = {}) {
  // Private flag to skip database storage (for direct API access without DB writes)
  const __noStore = params.__noStore === true;
  if (!ENDPOINTS[report]) throw new Error(`Unknown report type: ${report}`);

  // ---------------- Cache lookup ----------------
  const cacheKey = makeCacheKey(report, tenant, params);
  const cached = reportCache.get(cacheKey);
  if (cached && Date.now() < cached.expires) {
    console.log(`ðŸ“‹ Using cached data for ${report} (${cached.data.length || cached.data.rows?.length || 0} records)`);
    // Return a shallow copy so callers can mutate safely
    return Array.isArray(cached.data) ? [...cached.data] : cached.data;
  }
  // ------------------------------------------------

  // Use PUBLIC_URL for local endpoints (search) and BASE_URL for external API endpoints
  // Replace 'localhost' with '127.0.0.1' to avoid DNS resolution issues
  let publicUrl = process.env.PUBLIC_URL;
  if (report === 'search' && publicUrl.includes('localhost')) {
    publicUrl = publicUrl.replace('localhost', '127.0.0.1');
  }
  
  const url = report === 'search' 
    ? `${publicUrl}${ENDPOINTS[report]}` 
    : `${process.env.BASE_URL}${ENDPOINTS[report]}`;
  let token;
  const out = [];

  // Treat limit/maxRows as page size only (don't cap total rows)
  const pageSize = params.maxRows ?? params.limit ?? 500;

  // Normalized start key handling
  let startKey = params.start_key ?? params.startKey ?? undefined;

  // Track paging state
  const MAX_PAGES = 25000;              // hard safety cap for 8400+ records
  let pages = 0;

  console.log(`🎉 Starting complete pagination for ${report} endpoint`);
  console.log(`   URL: ${url}`);
  console.log(`   Params:`, JSON.stringify(params, null, 2));

  // Retry logic with exponential backoff
  retry: for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    const delay = Math.min(1000 * Math.pow(2, attempt), 10000);
    
    try {
      // Reset pagination state on retry
      if (attempt > 0) {
        startKey = params.start_key ?? params.startKey ?? undefined;
        pages = 0;
        out.length = 0;
        console.log(`ðŸ”„ ${report} - Retrying pagination from the beginning (attempt ${attempt + 1})`);
      }

      while (true) {
        const qs = {
          ...params,
          // Add account parameter for search endpoint
          ...(report === 'search' ? { account: tenant } : {}),
          // page size only; do NOT pass global caps here
          ...(report === 'cdrs' ? { maxRows: String(pageSize) } : {}),
          ...(report === 'queueCalls' || report === 'queueOutboundCalls' ? { maxRows: String(pageSize) } : {}),
          ...(report === 'campaignsActivity' ? { maxRows: String(pageSize) } : {}),
          ...(startKey && { startKey: startKey }),

          // Request full set of columns for queue reports so duration, abandon etc. are returned
          ...(report === 'queueOutboundCalls' && {
            fields: [
              'called_time',
              'agent_name',
              'agent_ext',
              'destination',
              'answered_time',
              'hangup_time',
              'wait_duration',
              'talked_duration',
              'queue_name',
              'queue_history',
              'agent_history',
              'agent_hangup',
              'call_id',
              'bleg_call_id',
              'event_timestamp',
              'agent_first_name',
              'agent_last_name',
              'agent_extension',
              'agent_email',
              'agent_talk_time',
              'agent_connect_time',
              'agent_action',
              'agent_transfer',
              'csat',
              'media_recording_id',
              'recording_filename',
              'caller_id_name',
              'caller_id_number',
              'a_leg',
              'interaction_id',
              'agent_disposition',
              'follow_up_notes',
              'agent_subdisposition',
              'to'
            ].join(',')
          }),
          // Same for inbound queue calls (queues_cdrs) so we get talked_duration & abandoned columns
          ...(report === 'queueCalls' && {
            fields: [
              'called_time',
              'caller_id_number',
              'caller_id_name',
              'answered_time',
              'hangup_time',
              'wait_duration',
              'talked_duration',
              'queue_name',
              'abandoned',
              'queue_history',
              'agent_history',
              'agent_attempts',
              'agent_hangup',
              'call_id',
              'bleg_call_id',
              'event_timestamp',
              'agent_first_name',
              'agent_last_name',
              'agent_extension',
              'agent_email',
              'agent_talk_time',
              'agent_connect_time',
              'agent_action',
              'agent_transfer',
              'csat',
              'media_recording_id',
              'recording_filename',
              'callee_id_number',
              'a_leg',
              'interaction_id',
              'follow_up_notes',
              'agent_disposition',
              'agent_subdisposition'
            ].join(',')
          }),
          // Request all relevant columns for campaign activity
          ...(report === 'campaignsActivity' && {
            fields: [
              'datetime',
              'timestamp',
              'campaign_name',
              'campaign_type',
              'lead_name',
              'lead_first_name',
              'lead_last_name',
              'lead_number',
              'lead_ticket_id',
              'lead_type',
              'agent_name',
              'agent_extension',
              'agent_talk_time',
              'lead_history',
              'agent_history',
              'call_id',
              'campaign_timestamps',
              'media_recording_id',
              'recording_filename',
              'status',
              'customer_wait_time_sla',
              'customer_wait_time_over_sla',
              'disposition',
              'hangup_cause',
              'lead_disposition',
              'agent_disposition',
              'agent_subdisposition',
              'follow_up_notes',
              'answered_time'
            ].join(',')
          }),
        };

        // token per-iteration (cached by tokenService)
        token = await getPortalToken(tenant);

        console.log(`ðŸ“Š ${report} - Page ${pages + 1} (attempt ${attempt + 1}/${MAX_RETRIES})`);
        const resp = await axios.get(url, {
          params: qs,
          headers: {
            Authorization: `Bearer ${token}`,
            'X-User-Agent': 'portal',
            'X-Account-ID': process.env.ACCOUNT_ID_HEADER ?? tenant
          },
          httpsAgent,
          timeout: 30000,
          validateStatus: s => s >= 200 && s < 300
        });

        const payload = resp.data;

        // Normalize rows
        let rows;
        if (Array.isArray(payload?.data))       rows = payload.data;
        else if (Array.isArray(payload))        rows = payload;
        else if (Array.isArray(payload?.rows))  rows = payload.rows;
        else                                    rows = Object.entries(payload).map(([k, v]) => ({ key: k, ...v }));

        // Store raw data immediately after each API call
        if (rows.length) {
          try {
            // Skip database storage if __noStore flag is set
            if (__noStore) {
              console.log(`â© Skipping storage of ${rows.length} raw ${report} records (noStore mode)`);
            } else {
              console.log(`ðŸ’¾ Storing ${rows.length} raw ${report} records from page ${pages + 1}...`);
              
              // Store based on report type without any processing
              if (report === 'campaignsActivity') {
                await Promise.all(rows.map(record => dbService.insertRawCampaigns(record)));
              } else if (report === 'queueCalls') {
                await Promise.all(rows.map(record => dbService.insertRawQueueInbound(record)));
              } else if (report === 'queueOutboundCalls') {
                await Promise.all(rows.map(record => dbService.insertRawQueueOutbound(record)));
              } else if (report === 'cdrs') {
                await Promise.all(rows.map(record => dbService.insertRawCdrs(record)));
              }
              
              console.log(`📊 Stored ${rows.length} raw ${report} records in database`);
            }
          } catch (error) {
            console.error(`âŒ Error storing raw ${report} data:`, error);
          }
          
          // Append to output for return
          out.push(...rows);
        }

        // Get next token in all common shapes
        let next = payload.next_start_key ?? payload.next ?? payload.nextKey ?? null;

        // Normalize terminators: null/undefined/"" => end
        const isTerminal = (next == null) || (next === '');
        pages++;

        console.log(`   ðŸ“Š Received ${rows.length} rows`);
        console.log(`   ðŸ”‘ Next start key: ${JSON.stringify(next)} (type: ${typeof next})`);
        console.log(`   ðŸ“Š Total rows so far: ${out.length}`);

        // End conditions
        if (isTerminal) {
          console.log(`📊 ${report} - fully drained (${pages} pages).`);
          break;
        }

        // Continue fetching with new token - no cycle detection needed
        // The API will naturally terminate when next_start_key becomes null/empty
        console.log(`   âž¡ï¸  Continuing with next_start_key: "${next}"`);

        if (pages >= MAX_PAGES) {
          console.warn(`ðŸ›‘ ${report} - Max page cap (${MAX_PAGES}) reached. Breaking defensively.`);
          break;
        }

        // Advance to next token
        startKey = next;
      }

      // If we reach here, pagination completed successfully
      break retry;

    } catch (err) {
      console.error(`âŒ ${report} - Error on attempt ${attempt + 1}/${MAX_RETRIES}:`, err.message);
      
      // Special handling for search endpoint errors
      if (report === 'search') {
        // Handle 404 errors
        if (err.response && err.response.status === 404) {
          console.error(`âš ï¸ Search endpoint not found. This is likely because the search endpoint is a local endpoint.`);
          console.error(`âš ï¸ Make sure the server is running and the PUBLIC_URL (${process.env.PUBLIC_URL}) is correct.`);
          console.error(`âš ï¸ Current search URL: ${url}`);
          throw new Error(`Search endpoint not found at ${url}. Make sure the server is running and the PUBLIC_URL is correct.`);
        }
        
        // Handle DNS resolution errors
        if (err.code === 'ENOTFOUND') {
          console.error(`âš ï¸ DNS resolution error: Could not resolve hostname in URL: ${url}`);
          console.error(`âš ï¸ Try using IP address 127.0.0.1 instead of 'localhost' in PUBLIC_URL.`);
          throw new Error(`DNS resolution error for ${url}. Try using IP address 127.0.0.1 instead of 'localhost' in PUBLIC_URL.`);
        }
      }
      
      if (attempt === MAX_RETRIES - 1) {
        console.error(`ðŸ’¥ ${report} - All retry attempts failed. Throwing error.`);
        throw err;
      }
      
      console.log(`ðŸ”„ ${report} - Retrying in ${delay}ms...`);
      await new Promise(r => setTimeout(r, delay));
    }
  }

  console.log(`ðŸŽ¯ ${report} - Final result: ${out.length} records fetched successfully`);

  // ---------------------------------------------------------------------------
  // Post-processing helpers
  // Add Extension column logic for all report types
  out.forEach(record => {
    let extension = '';
    
    if (report === 'campaignsActivity') {
      // For Campaign: use agent_extension from main JSON response
      extension = record.agent_extension || '';
    } else if (report === 'queueCalls' || report === 'queueOutboundCalls') {
      // For Inbound/Outbound: get ext from first agent_history record
      if (Array.isArray(record.agent_history) && record.agent_history.length > 0) {
        const firstAgentHistory = record.agent_history[0];
        extension = firstAgentHistory.ext || '';
      }
    } else if (report === 'cdrs') {
      // For CDRs: leave blank
      extension = '';
    }
    
    // Add Extension column to the record
    record.Extension = extension;
  });

  out.forEach(record => {
    let country = '';
    
    if (report === 'campaignsActivity') {
      // For Campaign: use lead_number from main JSON response
      country = extractCountryFromPhoneNumber(record.lead_number) || '';
    } else if (report === 'queueCalls') {
      // For Inbound calls: get country from caller_id_number (the person calling in)
      country = extractCountryFromPhoneNumber(record.caller_id_number) || '';
    } else if (report === 'queueOutboundCalls') {
      // For Outbound calls: get country from 'to' field (the person being called)
      const phoneNumber = record.to || record.destination || record.callee_id_number ;
      country = extractCountryFromPhoneNumber(phoneNumber) || '';
    } else if (report === 'cdrs') {
      // For CDRs: get country from caller_id_number or callee_id_number
      country = extractCountryFromPhoneNumber(record.caller_id_number) || extractCountryFromPhoneNumber(record.callee_id_number) || '';
    }
    
    // Add Country column to the record
    record.Country = country;
    
    // Add CSAT column to the record
    const csat = extractCSATFromAgentHistory(record.agent_history);
    record.CSAT = csat;
  });

  // ---------------------------------------------------------------------------
  // Helper utils for CDR enrichment (mirrors frontend formatting)
  function isoToLocal(dateStr) {
    return new Date(dateStr).toLocaleString('en-GB', { timeZone: 'Asia/Dubai' });
  }
  function formatTimestamp(ts) {
    if (ts == null || ts === '') return '';
    const ms = ts < 10_000_000_000 ? ts * 1000 : ts; // epoch secâ†’ms
    return isoToLocal(new Date(ms).toISOString());
  }
  function formatDuration(sec) {
    const total = parseInt(sec, 10);
    if (Number.isNaN(total)) return '';
    const h = Math.floor(total / 3600).toString().padStart(2, '0');
    const m = Math.floor((total % 3600) / 60).toString().padStart(2, '0');
    const s = (total % 60).toString().padStart(2, '0');
    return `${h}:${m}:${s}`;
  }
  // Simple HTML button creator (non-interactive for CSV/CLI but keeps parity)
  function createEyeBtn(html) { return html; }
  // History HTML helpers (condensed from frontend)
  function historyToHtml(hist){ if(!Array.isArray(hist)||!hist.length) return ''; const rows=hist.sort((a,b)=>(a.last_attempt??0)-(b.last_attempt??0)).map(h=>`<tr><td>${h.last_attempt?isoToLocal(new Date((h.last_attempt>10_000_000_000?h.last_attempt:h.last_attempt*1000)).toISOString()):''}</td><td>${(h.first_name||'')+' '+(h.last_name||'')}</td><td>${h.ext??''}</td><td>${h.type??''}</td><td>${h.event??''}</td><td>${h.connected?'Yes':'No'}</td><td>${h.queue_name??''}</td></tr>`).join(''); return `<table><tbody>${rows}</tbody></table>`; }
  function queueHistoryToHtml(hist){ if(!Array.isArray(hist)||!hist.length) return ''; const rows=hist.map(h=>`<tr><td>${h.ts?isoToLocal(new Date((h.ts>10_000_000_000?h.ts:h.ts*1000)).toISOString()):''}</td><td>${h.queue_name??''}</td></tr>`).join(''); return `<table><tbody>${rows}</tbody></table>`; }
  function leadHistoryToHtml(hist){ if(!Array.isArray(hist)||!hist.length) return ''; const rows=hist.sort((a,b)=>(a.last_attempt??0)-(b.last_attempt??0)).map(h=>`<tr><td>${h.last_attempt?isoToLocal(new Date((h.last_attempt>10_000_000_000?h.last_attempt:h.last_attempt*1000)).toISOString()):''}</td><td>${h.agent?.first_name??''}</td><td>${h.agent?.last_name??''}</td><td>${h.agent?.ext??''}</td><td>${h.type||h.event||''}</td><td>${h.hangup_cause||''}</td></tr>`).join(''); return `<table><tbody>${rows}</tbody></table>`; }
  function extractCampaignAgentName(agentHistory){ if(!agentHistory) return ''; try{ if(typeof agentHistory==='string') agentHistory=JSON.parse(agentHistory); }catch{return'';} if(!Array.isArray(agentHistory)) return ''; const ent=agentHistory.find(e=>e.event==='agent_answer'); if(!ent) return ''; return `${ent.first_name??''} ${ent.last_name??''}`.trim(); }

  function hasAgentHangupEvent(hist){
    if(!hist) return 'No';
    try{ if(typeof hist==='string') hist=JSON.parse(hist);}catch{return 'No';}
    if(Array.isArray(hist)){
      return hist.some(e=>e.event==='agent_hangup') ? 'Yes' : 'No';
    }
    return 'No';
  }

  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------

  // Additional enrichment for raw CDR records so the frontend gets pre-formatted values
  if (report === 'cdrs') {
    out.forEach(record => {
      record.Extension = record.extension ?? record.Extension ?? '';
      record['Agent name'] = extractCampaignAgentName(record.agent_history) || (record.agent_name ?? '');
      record['Agent Disposition'] = record.agent_disposition ?? '';
      record['Sub_disp_1'] = record.sub_disp_1 ?? record.sub_disposition_1 ?? '';
      record['Sub_disp_2'] = record.sub_disp_2 ?? record.sub_disposition_2 ?? '';
      record['Sub_disp_3'] = record.sub_disp_3 ?? record.sub_disposition_3 ?? '';
      record['Answered time'] = record.answered_time ? formatTimestamp(record.answered_time) : '';
      record['Hangup time'] = record.hangup_time ? formatTimestamp(record.hangup_time) : '';
      record['Wait Duration'] = record.wait_duration ? formatDuration(record.wait_duration) : '';
      record['Hold Duration'] = record.hold_duration ? formatDuration(record.hold_duration) : '';
      record['Agent Hangup'] = hasAgentHangupEvent(record.agent_history) ? 'Yes' : 'No';
      record['Status'] = record.hangup_cause ?? record.status ?? '';
      record['Campaign Type'] = record.campaign_type ?? '';
      record['Abandoned'] = record.abandoned ?? '';
      record['Agent History'] = `${historyToHtml(record.agent_history ?? [])}${leadHistoryToHtml(record.lead_history ?? [])}`;
      record['Queue History'] = Array.isArray(record.queue_history) ? queueHistoryToHtml(record.queue_history) : '';
      record.CSAT = record.csat ?? '';
    });
  }
// ---------------------------------------------------------------------------

// Add Agent Hangup column logic for all report types
  out.forEach(record => {
    let agentHangup = 'No';
    
    if (report === 'queueCalls' || report === 'queueOutboundCalls') {
      // For Inbound/Outbound: check if agent_history contains agent_hangup event
      if (Array.isArray(record.agent_history) && record.agent_history.length > 0) {
        // Check if any event in agent_history has event: "agent_hangup"
        const hasAgentHangup = record.agent_history.some(entry => entry.event === 'agent_hangup');
        agentHangup = hasAgentHangup ? 'Yes' : 'No';
      }
    } else if (report === 'campaignsActivity') {
      // For Campaign calls: check if agent_hangup event comes before lead_hangup event
      if (Array.isArray(record.agent_history) && record.agent_history.length > 0) {
        let agentHangupIndex = -1;
        let leadHangupIndex = -1;
        
        // Find indices of agent_hangup and lead_hangup events
        record.agent_history.forEach((entry, index) => {
          if (entry.event === 'agent_hangup') {
            agentHangupIndex = index;
          } else if (entry.event === 'lead_hangup') {
            leadHangupIndex = index;
          }
        });
        
        // If agent_hangup exists and comes before lead_hangup (or lead_hangup doesn't exist)
        if (agentHangupIndex !== -1 && (leadHangupIndex === -1 || agentHangupIndex < leadHangupIndex)) {
          agentHangup = 'Yes';
        }
      }
    }
    
    // Add Agent Hangup column to the record
    record['Agent Hangup'] = agentHangup;
  });

  if (report === 'queueCalls' || report === 'queueOutboundCalls') {
    // Derive durations if the backend omitted them (older Talkdesk tenants)
    out.forEach(record => {
      // Talked duration
      if (!record.talked_duration && record.hangup_time && record.answered_time) {
        record.talked_duration = record.hangup_time - record.answered_time;
      }
      // Wait / queue duration
      if (!record.wait_duration && record.called_time) {
        if (record.answered_time) {
          record.wait_duration = record.answered_time - record.called_time;
        } else if (record.hangup_time) {
          record.wait_duration = record.hangup_time - record.called_time;
        }
      }
    });
  }

  // For inbound queue reports Talkdesk returns one row per agent leg.
  // When the consumer only needs a single row per call we keep the *first*
  // occurrence for each call_id (usually the initial `dial` leg) and drop the rest.
  if (report === 'queueCalls') {
    const seen = new Set();
    const firstRows = [];
    for (const rec of out) {
      // If the row is missing a call_id we cannot group it â€“ keep it.
      if (!rec.call_id) {
        firstRows.push(rec);
        continue;
      }
      if (!seen.has(rec.call_id)) {
        seen.add(rec.call_id);
        firstRows.push(rec);
      }
    }

    // Derive `abandoned` flag when Talkdesk omits it
    // Business rule: if agent_history missing/empty, OR
    //                answered_time is falsy (not set), OR
    //                all agent_history entries lack answered_time
    firstRows.forEach(r => {
      const hist = r.agent_history;
      const histMissing =
        hist == null ||
        (Array.isArray(hist) && hist.length === 0) ||
        // Handle cases where API returns array with empty objects [{}]
        (Array.isArray(hist) && hist.every(h => h && Object.keys(h).length === 0));

      let histNoAnswer = false;
      if (Array.isArray(hist) && hist.length > 0) {
        histNoAnswer = hist.every(h => !h?.answered_time && !h?.agent_action?.includes("transfer"));
      }

      const isAbandoned = histMissing || !r.answered_time || histNoAnswer;

      // Always override to ensure consistency
      r.abandoned = isAbandoned ? "YES" : "NO";
    });

    // Store inbound queue data in raw_queue_inbound table BEFORE returning
    try {
      console.log(`ðŸ’¾ Storing ${firstRows.length} queueCalls records in raw database...`);
      const batchSize = 100;
      const totalBatches = Math.ceil(firstRows.length / batchSize);
      
      for (let i = 0; i < totalBatches; i++) {
        const batch = firstRows.slice(i * batchSize, (i + 1) * batchSize);
        await Promise.all(batch.map(record => dbService.insertRawQueueInbound(record)));
        console.log(`📊 Batch ${i+1}/${totalBatches}: Stored ${batch.length} inbound queue records in raw_queue_inbound table`);
      }
      console.log(`📊 Total: Stored ${firstRows.length} inbound queue records in raw_queue_inbound table`);
    } catch (error) {
      console.error(`âŒ Error storing queueCalls data in database:`, error);
    }

    // Cache result BEFORE returning
    reportCache.set(cacheKey, { expires: Date.now() + CACHE_TTL, data: firstRows });
    return { rows: firstRows, next: null };
  }

  // ---- Final time ordering (stable) ----
  const keyOrder = [
    // most common timestamp candidates in your payloads (fallbacks in order)
    'event_timestamp', 'called_time', 'answered_time', 'hangup_time', 'timestamp', 'datetime'
  ];
  const ts = r => {
    for (const k of keyOrder) {
      if (r[k] != null && r[k] !== '') return Number(r[k]);
    }
    return Number.NEGATIVE_INFINITY;
  };
  out.sort((a, b) => ts(a) - ts(b));
  
  console.log(`ðŸ“… ${report} - Records sorted chronologically by timestamp. Date range: ${
    out.length > 0 ? 
    `${new Date(ts(out[0]) * (ts(out[0]) < 10_000_000_000 ? 1000 : 1)).toISOString()} to ${new Date(ts(out[out.length-1]) * (ts(out[out.length-1]) < 10_000_000_000 ? 1000 : 1)).toISOString()}` : 
    'No records'
  }`);

  // Store data in raw tables based on report type
  try {
    // Skip database storage if __noStore flag is set
    if (__noStore) {
      console.log(`â© Skipping final storage of ${out.length} ${report} records (noStore mode)`);
    } else {
      console.log(`ðŸ’¾ Storing ${out.length} ${report} records in raw database...`);
      
      // Use Promise.all for better performance with large datasets
      const batchSize = 100; // Process records in batches to avoid memory issues
      const totalBatches = Math.ceil(out.length / batchSize);
      
      if (report === 'campaignsActivity') {
      // Filter to keep only records that have essential campaign fields populated
      const trueCampaignRecords = out.filter(record => {
        // Check if record has essential campaign fields that indicate it's a real campaign record
        const hasAgentExtension = record.agent_extension && record.agent_extension.trim() !== '';
        const hasLeadData = record.lead_number && record.lead_number.trim() !== '';
        const hasCampaignType = record.campaign_type && record.campaign_type.trim() !== '';
        const hasAgentName = record.agent_name && record.agent_name.trim() !== '';
        
        // A true campaign record should have these essential fields
        return hasAgentExtension && hasLeadData && hasCampaignType && hasAgentName;
      });
      
      console.log(`  Filtered campaigns: ${out.length} total    ${trueCampaignRecords.length} true campaign records`);
      
      // Store only true campaign data in raw_campaigns table
      const filteredBatches = Math.ceil(trueCampaignRecords.length / batchSize);
      for (let i = 0; i < filteredBatches; i++) {
        const batch = trueCampaignRecords.slice(i * batchSize, (i + 1) * batchSize);
        await Promise.all(batch.map(record => dbService.insertRawCampaigns(record)));
        console.log(`📊 Batch ${i+1}/${filteredBatches}: Stored ${batch.length} campaign records in raw_campaigns table`);
      }
      console.log(`📊 Total: Stored ${trueCampaignRecords.length} campaign records in raw_campaigns table`);
      
      // Update out array to only include true campaign records
      out = trueCampaignRecords;
    } 
    else if (report === 'queueCalls') {
      // Store inbound queue data in raw_queue_inbound table
      for (let i = 0; i < totalBatches; i++) {
        const batch = out.slice(i * batchSize, (i + 1) * batchSize);
        await Promise.all(batch.map(record => dbService.insertRawQueueInbound(record)));
        console.log(`📊 Batch ${i+1}/${totalBatches}: Stored ${batch.length} inbound queue records in raw_queue_inbound table`);
      }
      console.log(`📊 Total: Stored ${out.length} inbound queue records in raw_queue_inbound table`);
    } 
    else if (report === 'queueOutboundCalls') {
      // Store outbound queue data in raw_queue_outbound table
      for (let i = 0; i < totalBatches; i++) {
        const batch = out.slice(i * batchSize, (i + 1) * batchSize);
        await Promise.all(batch.map(record => dbService.insertRawQueueOutbound(record)));
        console.log(`📊 Batch ${i+1}/${totalBatches}: Stored ${batch.length} outbound queue records in raw_queue_outbound table`);
      }
      console.log(`📊 Total: Stored ${out.length} outbound queue records in raw_queue_outbound table`);
    } 
    else if (report === 'cdrs') {
      // Store CDRs data in raw_cdrs table
      for (let i = 0; i < totalBatches; i++) {
        const batch = out.slice(i * batchSize, (i + 1) * batchSize);
        await Promise.all(batch.map(record => dbService.insertRawCdrs(record)));
        console.log(`📊 Batch ${i+1}/${totalBatches}: Stored ${batch.length} CDRs records in raw_cdrs table`);
      }
      console.log(`📊 Total: Stored ${out.length} CDRs records in raw_cdrs table`);
    }
  }
  } catch (error) {
    console.error(`📊 Error storing ${report} data in database:`, error);
  }

  // Cache result BEFORE returning
  reportCache.set(cacheKey, { expires: Date.now() + CACHE_TTL, data: out });
  return { rows: out, next: null };
}

// Convenience wrappers
export const fetchCdrs = (tenant, opts) => fetchReportFromAPI('cdrs', tenant, opts);
export const fetchQueueCalls = (tenant, opts) => fetchReportFromAPI('queueCalls', tenant, opts);
export const fetchQueueOutboundCalls = (tenant, opts) => fetchReportFromAPI('queueOutboundCalls', tenant, opts);
export const fetchCampaignsActivity = (tenant, opts) => fetchReportFromAPI('campaignsActivity', tenant, opts);

// Helper functions for data processing (from fetchReport)
function isoToLocal(dateStr) {
  return new Date(dateStr).toLocaleString('en-GB', { timeZone: 'Asia/Dubai' });
}

function formatTimestamp(ts) {
  if (ts == null || ts === '' || ts === undefined) return '';
  
  try {
    // Validate timestamp is a number
    let timestamp = ts;
    if (typeof ts === 'string') {
      timestamp = parseFloat(ts);
      if (isNaN(timestamp)) return '';
    }
    
    if (typeof timestamp !== 'number' || isNaN(timestamp)) return '';
    
    // Validate timestamp range
    if (timestamp < 0 || timestamp > 4102444800000) return '';
    
    const ms = timestamp < 10_000_000_000 ? timestamp * 1000 : timestamp; // epoch secâ†’ms
    const date = new Date(ms);
    
    if (isNaN(date.getTime())) return '';
    
    return isoToLocal(date.toISOString());
  } catch (error) {
    console.warn('Error formatting timestamp:', ts, error);
    return '';
  }
}

function formatDuration(sec) {
  const total = parseInt(sec, 10);
  if (Number.isNaN(total)) return '';
  const h = Math.floor(total / 3600).toString().padStart(2, '0');
  const m = Math.floor((total % 3600) / 60).toString().padStart(2, '0');
  const s = (total % 60).toString().padStart(2, '0');
  return `${h}:${m}:${s}`;
}

/**
 * Process campaign record for both frontend display and final report
 * @param {Object} record - Raw campaign record
 * @param {Object} options - Processing options
 * @param {boolean} options.forReport - If true, format for final_report table; if false, format for frontend display
 * @param {string} options.sub1 - First subdisposition (for final_report)
 * @param {string} options.sub2 - Second subdisposition (for final_report)
 * @returns {Object} - Processed campaign record
 */
function processCampaignRecord(record, { forReport = false, sub1 = '', sub2 = '' } = {}) {
  // Helper functions for campaign data processing
  function formatTimestamp(ts) {
    if (ts == null || ts === '') return '';
    let timestamp = ts;
    if (typeof ts === 'string') {
      timestamp = parseFloat(ts);
    }
    const ms = timestamp < 10_000_000_000 ? timestamp * 1000 : timestamp;
    const date = new Date(ms);
    if (isNaN(date.getTime())) return '';
    return date.toLocaleString('en-GB', { timeZone: 'Asia/Dubai', day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
  }
  
  function formatDuration(sec) {
    const total = parseInt(sec, 10);
    if (Number.isNaN(total)) return '';
    const h = Math.floor(total / 3600).toString().padStart(2, '0');
    const m = Math.floor((total % 3600) / 60).toString().padStart(2, '0');
    const s = (total % 60).toString().padStart(2, '0');
    return `${h}:${m}:${s}`;
  }
  
  // Helper to wrap arbitrary HTML in an eye button that opens a modal (frontend only)
  function createEyeBtn(innerHtml) {
    const id = 'popup_' + Math.random().toString(36).slice(2, 9);
    return `<button class="button is-small is-rounded eye-btn" data-target="${id}" title="View">&#128065;</button>` +
           `<div id="${id}" class="popup-content" style="display:none">${innerHtml}</div>`;
  }
  
  function historyToHtml(hist){ 
    if(!Array.isArray(hist)||!hist.length) return ''; 
    const sorted = [...hist].sort((a,b)=>(a.last_attempt??0)-(b.last_attempt??0));
    const COLS = [
      { key: 'last_attempt', label: 'Last Attempt' },
      { key: 'name', label: 'Name' },
      { key: 'ext', label: 'Extension' },
      { key: 'type', label: 'Type' },
      { key: 'event', label: 'Event' },
      { key: 'connected', label: 'Connected' },
      { key: 'queue_name', label: 'Queue Name' }
    ];
    const thead = `<thead><tr>${COLS.map(c => `<th>${c.label}</th>`).join('')}</tr></thead>`;
    const rows = sorted.map(h => {
      const cells = COLS.map(c => {
        let val = '';
        if (c.key === 'name') {
          val = `${h.first_name || ''} ${h.last_name || ''}`.trim();
        } else if (c.key === 'last_attempt') {
          if (h.last_attempt) {
            const ms = h.last_attempt > 10_000_000_000 ? h.last_attempt : h.last_attempt * 1000;
            val = new Date(ms).toLocaleString('en-GB', { timeZone: 'Asia/Dubai' });
          }
        } else if (c.key === 'connected') {
          val = h.connected ? 'Yes' : 'No';
        } else {
          val = h[c.key] ?? '';
        }
        return `<td>${val}</td>`;
      }).join('');
      return `<tr>${cells}</tr>`;
    }).join('');
    const tableHtml = `<table class="history-table">${thead}<tbody>${rows}</tbody></table>`;
    return createEyeBtn(tableHtml);
  }
  
  function leadHistoryToHtml(hist){ 
    if(!Array.isArray(hist)||!hist.length) return ''; 
    const sorted = [...hist].sort((a,b)=>(a.last_attempt??0)-(b.last_attempt??0));
    const thead = '<thead><tr><th>Last Attempt</th><th>First Name</th><th>Last Name</th><th>Extension/Number</th><th>Event</th><th>Hangup Cause</th></tr></thead>';
    const rows = sorted.map(h => {
      let last = '';
      if (h.last_attempt) {
        const ms = h.last_attempt > 10_000_000_000 ? h.last_attempt : h.last_attempt * 1000;
        last = new Date(ms).toLocaleString('en-GB', { timeZone: 'Asia/Dubai' });
      }
      const fn = h.agent?.first_name ?? '';
      const ln = h.agent?.last_name ?? '';
      const ext = h.agent?.ext ?? '';
      const evt = h.type || h.event || '';
      const cause = h.hangup_cause || '';
      return `<tr><td>${last}</td><td>${fn}</td><td>${ln}</td><td>${ext}</td><td>${evt}</td><td>${cause}</td></tr>`;
    }).join('');
    const tableHtml = `<table class="history-table">${thead}<tbody>${rows}</tbody></table>`;
    return createEyeBtn(tableHtml);
  }
  
  function extractCampaignAgentName(agentHistory){ 
    if(!agentHistory) return ''; 
    try{ 
      if(typeof agentHistory==='string') agentHistory=JSON.parse(agentHistory); 
    }catch{return'';} 
    if(!Array.isArray(agentHistory)) return ''; 
    const ent=agentHistory.find(e=>e.event==='agent_answer'); 
    if(!ent) return ''; 
    return `${ent.first_name??''} ${ent.last_name??''}`.trim(); 
  }
  
  function hasAgentHangupEvent(hist){
    if(!hist) return 'No';
    try{ if(typeof hist==='string') hist=JSON.parse(hist);}catch{return 'No';}
    if(Array.isArray(hist)){
      return hist.some(e=>e.event==='agent_hangup') ? 'Yes' : 'No';
    }
    return 'No';
  }

  /**
   * Calculate hold duration from agent_history events
   * @param {Array} agentHistory - Agent history array
   * @param {number} hangupTime - Call hangup time
   * @returns {string} - Hold duration in seconds or empty string
   */
  function calculateHoldDuration(agentHistory, hangupTime) {
    if (!Array.isArray(agentHistory) || agentHistory.length === 0) {
      return '';
    }
    
    let totalHoldDuration = 0;
    let holdPairs = [];
    
    // First, find all hold_start and hold_stop pairs
    for (let i = 0; i < agentHistory.length; i++) {
      const event = agentHistory[i];
      
      if (event.event === 'hold_start' && event.last_attempt) {
        const holdStartTime = event.last_attempt;
        let holdEndTime = null;
        let matchFound = false;
        
        // Look for corresponding hold_stop event
        for (let j = i + 1; j < agentHistory.length; j++) {
          const nextEvent = agentHistory[j];
          if (nextEvent.event === 'hold_stop' && nextEvent.last_attempt) {
            holdEndTime = nextEvent.last_attempt;
            matchFound = true;
            break;
          }
        }
        
        // If no matching hold_stop found, use hangup_time as fallback
        if (!matchFound && hangupTime) {
          holdEndTime = typeof hangupTime === 'number' ? hangupTime : parseFloat(hangupTime);
        }
        
        // Add valid hold pair to our collection
        if (holdStartTime && holdEndTime && holdEndTime > holdStartTime) {
          holdPairs.push({
            start: holdStartTime,
            end: holdEndTime,
            duration: holdEndTime - holdStartTime
          });
        }
      }
    }
    
    // Calculate total hold duration from all pairs
    if (holdPairs.length > 0) {
      totalHoldDuration = holdPairs.reduce((total, pair) => total + pair.duration, 0);
      console.log(`Hold duration calculation: Found ${holdPairs.length} hold periods totaling ${totalHoldDuration.toFixed(2)} seconds`);
      
      // Log details of each hold period for debugging
      holdPairs.forEach((pair, index) => {
        console.log(`Hold period ${index + 1}: ${pair.start} to ${pair.end} (${pair.duration.toFixed(2)} seconds)`);
      });
    }
    
    // Return the total hold duration in seconds
    return totalHoldDuration > 0 ? totalHoldDuration.toFixed(2) : '';
  }
  
  // Extract subdisposition data with enhanced error handling (including third level)
  const [extractedSub1, extractedSub2, extractedSub3] = (() => {
    try {
      let sd = record.agent_subdisposition ?? null;
      
      // Handle different data types
      if (Array.isArray(sd)) {
        sd = sd[0]; // Take first element if array
      }
      
      // Handle string values (sometimes API returns strings)
      if (typeof sd === 'string') {
        try {
          sd = JSON.parse(sd);
        } catch (parseError) {
          // If not JSON, treat as plain string for sub_disp_1
          return [sd, '', ''];
        }
      }
      
      // Handle null, undefined, or non-object values
      if (!sd || typeof sd !== 'object') {
        return ['', '', ''];
      }
      
      // Extract nested subdisposition data (including third level)
      const first = sd.name ?? sd.subdisposition_name ?? sd.disposition ?? '';
      const second = sd.subdisposition?.name ?? sd.subdisposition?.subdisposition_name ?? '';
      const third = sd.subdisposition?.subdisposition?.name ?? '';
      
      // Ensure strings are returned (not objects)
      const firstStr = typeof first === 'object' ? JSON.stringify(first) : String(first || '');
      const secondStr = typeof second === 'object' ? JSON.stringify(second) : String(second || '');
      const thirdStr = typeof third === 'object' ? JSON.stringify(third) : String(third || '');
      
      return [firstStr, secondStr, thirdStr];
    } catch (error) {
      console.warn('Error extracting subdisposition data:', error, 'Record:', record.call_id);
      return ['', '', ''];
    }
  })();
  
  // Use provided subdispositions for final_report or extracted ones for frontend
  const finalSub1 = forReport ? (sub1 || extractedSub1) : extractedSub1;
  const finalSub2 = forReport ? (sub2 || extractedSub2) : extractedSub2;
  const finalSub3 = extractedSub3;
  
  // Sanitize subdisposition fields for final_report
  const sanitizedSub1 = forReport ? 
    (typeof finalSub1 === 'object' && finalSub1 !== null ? (finalSub1.name || JSON.stringify(finalSub1)) : (finalSub1 || '')) :
    finalSub1;
  const sanitizedSub2 = forReport ? 
    (typeof finalSub2 === 'object' && finalSub2 !== null ? (finalSub2.name || JSON.stringify(finalSub2)) : (finalSub2 || '')) :
    finalSub2;
  const sanitizedSub3 = forReport ? 
    (typeof finalSub3 === 'object' && finalSub3 !== null ? (finalSub3.name || JSON.stringify(finalSub3)) : (finalSub3 || '')) :
    finalSub3;
  
  // Extract agent name
  let agentName = extractCampaignAgentName(record.agent_history) || (record.agent_name ?? '');
  
  // Extract extension
  let agentExt = record.agent_extension ?? '';
  
  // Parse agent_history for additional data (frontend only)
  let agentHistory = record.agent_history;
  let agentHistoryHtml = '';
  let leadHistoryHtml = '';
  let combinedHistory = '';
  
  if (!forReport) {
    // Frontend-specific processing
    if (agentHistory) {
      try {
        if (typeof agentHistory === 'string') {
          agentHistory = JSON.parse(agentHistory);
        }
        if (Array.isArray(agentHistory)) {
          agentHistoryHtml = historyToHtml(agentHistory);
        }
      } catch (e) {
        console.warn('Failed to parse agent_history:', e);
      }
    }
    
    // Parse lead_history for queue history (frontend only)
    let leadHistory = record.lead_history;
    if (leadHistory) {
      try {
        if (typeof leadHistory === 'string') {
          leadHistory = JSON.parse(leadHistory);
        }
        if (Array.isArray(leadHistory)) {
          leadHistoryHtml = leadHistoryToHtml(leadHistory);
        }
      } catch (e) {
        console.warn('Failed to parse lead_history:', e);
      }
    }
    
    // Combine agent and lead history
    combinedHistory = `${agentHistoryHtml}${leadHistoryHtml}`;
  }
  
  // Calculate times from agent_history if available
  let answeredTime = '';
  let hangupTime = '';
  
  // First try campaign_timestamps (new format)
  if (record.campaign_timestamps) {
    if (record.campaign_timestamps.lead_answer_time) {
      answeredTime = formatTimestamp(record.campaign_timestamps.lead_answer_time);
    }
    if (record.campaign_timestamps.lead_hangup_time) {
      hangupTime = formatTimestamp(record.campaign_timestamps.lead_hangup_time);
    }
  }
  
  // Fallback to lead_history array (alternative format)
  if (!answeredTime || !hangupTime) {
    if (Array.isArray(record.lead_history)) {
      if (!answeredTime) {
        const answerEvent = record.lead_history.find(e => e.type === 'lead_answer');
        if (answerEvent && answerEvent.last_attempt) {
          answeredTime = formatTimestamp(answerEvent.last_attempt);
        }
      }
      if (!hangupTime) {
        const hangupEvent = record.lead_history.find(e => e.type === 'lead_hangup');
        if (hangupEvent && hangupEvent.last_attempt) {
          hangupTime = formatTimestamp(hangupEvent.last_attempt);
        }
      }
    }
  }
  
  // Final fallback to agent_history (old format)
  if (!answeredTime || !hangupTime) {
    if (Array.isArray(record.agent_history)) {
      if (!answeredTime) {
        const answerEvent = record.agent_history.find(e => e.event === 'agent_answer');
        if (answerEvent && answerEvent.last_attempt) {
          answeredTime = formatTimestamp(answerEvent.last_attempt);
        }
      }
      if (!hangupTime) {
        const hangupEvent = record.agent_history.find(e => e.event === 'agent_hangup');
        if (hangupEvent && hangupEvent.last_attempt) {
          hangupTime = formatTimestamp(hangupEvent.last_attempt);
        }
      }
    }
  }
  
  // Calculate hold duration
  const holdDuration = calculateHoldDuration(record.agent_history, record.hangup_time);
  
  // Extract follow-up notes
  const followUpNotes = record.follow_up_notes || record['Follow up notes'] || '';
  
  // Extract country from lead_number for campaign calls
  const country = extractCountryFromPhoneNumber(record.lead_number) || '';
  
  // Format talk duration
  const talkDuration = record.agent_talk_time ? formatDuration(record.agent_talk_time) : '';
  
  if (forReport) {
    // Return format for final_report table
    return {
      'Type': 'Campaign',
      'Call ID': record.call_id ?? record.callid ?? '',
      'Queue / Campaign Name': record.campaign_name ?? record.queue_name ?? '',
      'Campaign Type': record.campaign_type ?? 'preview',
      'Caller ID': record.agent_extension ?? '',
      'Callee ID': record.lead_number ?? '',
      'Contact number': record.lead_number ?? '',
      'Agent name': agentName,
      'Caller ID Number': record.lead_number ?? '',  // For campaigns, this should be the lead number being called
      'Talk Duration': talkDuration,
      'Hold Duration': holdDuration,
      'Agent Hangup': record['Agent Hangup'] ?? hasAgentHangupEvent(record.agent_history),
      'Agent Disposition': (() => {
        const agentDisp = record.agent_disposition;
        if (typeof agentDisp === 'object' && agentDisp !== null) {
          return agentDisp.name || JSON.stringify(agentDisp);
        }
        return String(agentDisp || '');
      })(),
      'Disposition': (() => {
        const disp = record.disposition;
        if (typeof disp === 'object' && disp !== null) {
          return disp.name || JSON.stringify(disp);
        }
        return String(disp || '');
      })(),
      'Sub_disp_1': sanitizedSub1,
      'Sub_disp_2': sanitizedSub2,
      'Sub_disp_3': sanitizedSub3,
      'Follow up notes': followUpNotes,
      'Called Time': formatTimestamp(record.timestamp ?? record.datetime) ?? '',
      'Answered time': answeredTime || (formatTimestamp(record.agent_answer_time) ?? ''),
      'Hangup time': hangupTime || (formatTimestamp(record.agent_hangup_time) ?? ''),
      'Wait Duration': record.customer_wait_time_sla ? formatDuration(record.customer_wait_time_sla) : (record.wait_duration ? formatDuration(record.wait_duration) : ''),
      'Recording': record.media_recording_id ?? record.recording_filename ?? '',
      'Status': record.status ?? '',
      'Extension': record.agent_extension ?? '',
      'Country': country,
      'CSAT': record.csat ?? '',
    };
  } else {
    // Return format for frontend display (original format)
    return {
      'Type': 'Campaign',
      'Call ID': record.call_id ?? record.callid ?? '',
      'Queue / Campaign Name': record.campaign_name ?? record.queue_name ?? '',
      'Campaign Type': record.campaign_type ?? 'preview',
      'Caller ID': record.agent_extension ?? '',
      'Callee ID': record.lead_number ?? '',
      'Contact number': record.lead_number ?? '',
      'Agent name': agentName,
      'Caller ID Number': record.lead_number ?? '',
      'Talk Duration': talkDuration,
      'Hold Duration': holdDuration,
      'Agent Hangup': hasAgentHangupEvent(record.agent_history),
      'Agent Disposition': (() => {
        const agentDisp = record.agent_disposition;
        if (typeof agentDisp === 'object' && agentDisp !== null) {
          return agentDisp.name || JSON.stringify(agentDisp);
        }
        return String(agentDisp || '');
      })(),
      'Disposition': (() => {
        const disp = record.disposition;
        if (typeof disp === 'object' && disp !== null) {
          return disp.name || JSON.stringify(disp);
        }
        return String(disp || '');
      })(),
      'Sub_disp_1': sanitizedSub1,
      'Sub_disp_2': sanitizedSub2,
      'Sub_disp_3': sanitizedSub3,
      'Follow up notes': followUpNotes,
      'Called Time': formatTimestamp(record.timestamp ?? record.datetime) ?? '',
      'Answered time': answeredTime || (formatTimestamp(record.agent_answer_time) ?? ''),
      'Hangup time': hangupTime || (formatTimestamp(record.agent_hangup_time) ?? ''),
      'Wait Duration': record.customer_wait_time_sla ? formatDuration(record.customer_wait_time_sla) : (record.wait_duration ? formatDuration(record.wait_duration) : ''),
      'Recording': record.media_recording_id ?? record.recording_filename ?? '',
      'Status': record.status ?? '',
      'Extension': record.agent_extension ?? '',
      'Country': country,
      'Agent History': combinedHistory,
      'Queue History': combinedHistory,
      'CSAT': record.csat ?? '',
    };
  }
}

/**
 * Create unified report from database records
 * @param {string} tenant - Tenant identifier
 * @param {Object} params - Query parameters (startDate, endDate, etc.)
 * @returns {Promise<Object>} - Combined report with all data sorted by time
 */
export async function createUnifiedReportFromDB(tenant, params = {}) {
  console.log(' 📊 Creating unified report from database records...');
  
  try {
    let results;
    
    // Check if we need to filter by contact number at database level
    if (params.contactNumber && params.contactNumber.trim() !== '') {
      console.log(` 📊 Contact number filter detected: ${params.contactNumber}`);
      console.log(` 📊 Using database-level filtering for better performance...`);
      
      // Use the database filtering function for better performance
      const filteredData = await dbService.getRecordsByContactNumber({
        contactNumber: params.contactNumber,
        startDate: params.startDate || params.start_date,
        endDate: params.endDate || params.end_date
      });
      
      // Use the filtered records from database
      results = [
        filteredData.campaignRecords,
        filteredData.inboundRecords,
        filteredData.outboundRecords,
        filteredData.cdrRecords
      ];
      
      // Mark that we've already filtered at database level
      params._dbFiltered = true;
      
      console.log(`📊 Records filtered by contact number at database level:`);
      console.log(`   - Campaigns: ${filteredData.campaignRecords.length}`);
      console.log(`   - Inbound: ${filteredData.inboundRecords.length}`);
      console.log(`   - Outbound: ${filteredData.outboundRecords.length}`);
      console.log(`   - CDRs: ${filteredData.cdrRecords.length}`);
      console.log(`   - Total: ${filteredData.totalRecords} records`);
    } else {
      // Standard fetching without contact number filtering
      results = await Promise.all([
        dbService.getRawCampaigns(params),
        dbService.getRawQueueInbound(params),
        dbService.getRawQueueOutbound(params),
        // dbService.getRawCdrs(params),
      ]);
    }
    
    // Map the results to include record_type with standardized values
    const recordTypes = ['Campaign', 'Inbound', 'Outbound', 'CDR'];
    const typedResults = results.map((records, index) => {
      return records.map(record => ({ ...record, record_type: recordTypes[index] }));
    });
    
    // Parse JSON data for all records
    const parseRecords = (records) => {
      return records.map(record => {
        if (record.raw_data) {
          try {
            const parsedData = typeof record.raw_data === 'string' 
              ? JSON.parse(record.raw_data) 
              : record.raw_data;
            return { ...record, ...parsedData };
          } catch (e) {
            console.warn('Failed to parse raw_data JSON:', e.message);
            return record;
          }
        }
        return record;
      });
    };
    
    // Parse all record sets
    const [parsedCampaignRecords, parsedInboundRecords, parsedOutboundRecords] = 
      typedResults.map(parseRecords);
      

    
    // Debug record counts
    console.log(`📊 Raw record counts from database:`);
    console.log(`   - Campaign records: ${parsedCampaignRecords.length}`);
    console.log(`   - Inbound records: ${parsedInboundRecords.length}`);
    console.log(`   - Outbound records: ${parsedOutboundRecords.length}`);
    
    // Optional: Show sample record structure
    typedResults.forEach((result, index) => {
      if (result.length > 0) {
        console.log(`📊 Sample ${recordTypes[index]} keys:`, Object.keys(result[0]));
      }
    });
    
    // Process and normalize records with proper data transformation
    let allRecords = [];

    // Process campaign records
    if (parsedCampaignRecords.length === 0) {
      console.log('📊  No campaign records found in database');
    } else {
      console.log(`📊 Processing ${parsedCampaignRecords.length} campaign records...`);
      console.log('Sample campaign record structure:', parsedCampaignRecords[0]);
    }
    
    parsedCampaignRecords.forEach((row, index) => {
      if (!row.raw_data) {
        console.log(`📊  Campaign record ${index} missing raw_data:`, row);
        return;
      }
      
      const record = { ...row.raw_data, record_type: 'Campaign' };
      const processedRecord = processCampaignRecord(record);
      Object.assign(record, processedRecord);
      record._recordType = 'campaign';
      
      // Detect transfer events in lead_history for campaign calls
      if (Array.isArray(record.lead_history)) {
        const transferInfo = detectTransferEvents(record.lead_history, 'campaign', record);
        record.transfer_event = transferInfo.transfer_event;
        record.transfer_extension = transferInfo.transfer_extension;
        record.transfer_queue_extension = transferInfo.transfer_queue_extension;
        record.transfer_type = transferInfo.transfer_type;
        
        if (transferInfo.transfer_event) {
          console.log(`📊 Campaign call ${record.call_id} has transfer to extension ${transferInfo.transfer_extension}`);
        }
      }
      
      // Use direct follow_up_notes from record
      const followUpNotes = record.follow_up_notes || '';
      if (followUpNotes) {
        record['Follow up notes'] = followUpNotes;
      }
      
      allRecords.push(record);
    });
    
    // Process inbound queue records
    parsedInboundRecords.forEach((row, index) => {
      const record = { ...row.raw_data, record_type: 'Inbound' };
      const processedRecord = processRecordData(record, 'queueCalls');
      Object.assign(record, processedRecord);
      record._recordType = 'inbound';
      
      // Detect transfer events in agent_history for inbound calls
      if (Array.isArray(record.agent_history)) {
        // Debug: Log sample inbound call agent_history structure for first few calls
        if (index < 3 && record.agent_history.length > 0) {
          const callId = record.callid || record.call_id;
          console.log(`📊 INBOUND DEBUG: Call ${callId} has ${record.agent_history.length} agent_history events`);
          record.agent_history.forEach((event, idx) => {
            if (event && (event.type === 'transfer' || event.event === 'dial' || event.event === 'transfer')) {
              console.log(`📊 INBOUND Event ${idx}: type="${event.type}", event="${event.event}", ext="${event.ext}"`);
            }
          });
        }
        
        const transferInfo = detectTransferEvents(record.agent_history, 'inbound', record);
        record.transfer_event = transferInfo.transfer_event;
        record.transfer_extension = transferInfo.transfer_extension;
        record.transfer_queue_extension = transferInfo.transfer_queue_extension;
        record.transfer_type = transferInfo.transfer_type;
        
        if (transferInfo.transfer_event) {
          console.log(`📊 Inbound call ${record.callid || record.call_id} has transfer to extension ${transferInfo.transfer_extension}`);
        }
      }
      
      // Use direct follow_up_notes from record
      const followUpNotes = record.follow_up_notes || '';
      if (followUpNotes) {
        record['Follow up notes'] = followUpNotes;
      }
      
      allRecords.push(record);
    });
    
    // Process outbound queue records
    const transfersToFetch = [];
    
    parsedOutboundRecords.forEach(row => {
      const record = { ...row.raw_data, record_type: 'Outbound' };
      const processedRecord = processRecordData(record, 'queueOutboundCalls');
      Object.assign(record, processedRecord);
      record._recordType = 'outbound';
      
      // Detect transfer events in agent_history for outbound calls
      if (Array.isArray(record.agent_history)) {
        const transferInfo = detectTransferEvents(record.agent_history, 'outbound', record);
        record.transfer_event = transferInfo.transfer_event;
        record.transfer_extension = transferInfo.transfer_extension;
        record.transfer_queue_extension = transferInfo.transfer_queue_extension;
        record.transfer_type = transferInfo.transfer_type;
        
        if (transferInfo.transfer_event && transferInfo.transfer_extension && transferInfo.last_attempt) {
          console.log(`📊 Outbound call ${record.callid || record.call_id} has transfer to extension ${transferInfo.transfer_extension}`);
          
          // Add to the list of transfers to fetch
          transfersToFetch.push({
            callId: record.callid || record.call_id,
            transferInfo: {
              ...transferInfo,
              source_record: record
            }
          });
        }
      }
      
      // Use direct follow_up_notes from record
      const followUpNotes = record.follow_up_notes || '';
      if (followUpNotes) {
        record['Follow up notes'] = followUpNotes;
      }
      
      allRecords.push(record);
    });
    
    // Process transferred calls if any transfers were detected
    if (transfersToFetch.length > 0) {
      console.log(`ðŸ”„ Found ${transfersToFetch.length} transfers to fetch from CDR records`);
      
      // Import the enhanced CDR matching functions
      const { processAgentHistoryForTransfers } = await import('./apiDataFetcher.js');
      
      // Process each transfer sequentially to avoid overwhelming the database
      for (const transfer of transfersToFetch) {
        console.log(`ðŸ”„ Processing transfer for call ${transfer.callId} to extension ${transfer.transferInfo.transfer_extension}`);
        
        // First try using the enhanced-cdr-matching.js implementation
        const transferredCalls = await fetchTransferredCalls(transfer.transferInfo, tenant, params);
        
        // If no results, try the direct CDR lookup approach
        let allTransferredCalls = [...transferredCalls];
        
        // If we have agent_history data, process it for additional transferred calls
        if (transfer.transferInfo.source_record && 
            Array.isArray(transfer.transferInfo.source_record.agent_history) && 
            transfer.transferInfo.source_record.agent_history.length > 0) {
          
          console.log(`📊 Processing agent_history for additional transferred calls`);
          const agentHistoryTransfers = await processAgentHistoryForTransfers(
            transfer.transferInfo.source_record.agent_history,
            tenant,
            params
          );
          
          if (agentHistoryTransfers.length > 0) {
            console.log(`📊 Found ${agentHistoryTransfers.length} additional transferred calls from agent_history`);
            allTransferredCalls = [...allTransferredCalls, ...agentHistoryTransfers];
          }
        }
        
        // Remove duplicates based on call_id
        const uniqueTransferredCalls = [];
        const seenCallIds = new Set();
        
        allTransferredCalls.forEach(call => {
          const callId = call.call_id || call.callid;
          if (callId && !seenCallIds.has(callId)) {
            seenCallIds.add(callId);
            uniqueTransferredCalls.push(call);
          }
        });
        
        if (uniqueTransferredCalls.length > 0) {
          console.log(`📊 Found ${uniqueTransferredCalls.length} unique transferred call records for call ${transfer.callId}`);
          
          // Process each transferred call and add to allRecords
          uniqueTransferredCalls.forEach((transferredCall, index) => {
            // Create a record with proper structure
            const record = {
              ...transferredCall,
              record_type: 'Transferred CDR',
              _recordType: 'transferred_cdr',
              Type: 'Transferred CDR',
              transfer_event: true,
              transfer_extension: transfer.transferInfo.transfer_extension,
              transfer_type: transfer.transferInfo.transfer_type,
              transfer_source_call_id: transfer.callId,
              source_record: transfer.transferInfo.source_record
            };
            
            // Process the record like a CDR record
            const processedRecord = processCDRRecord(transferredCall);
            Object.assign(record, processedRecord);
            
            // Add to allRecords
            allRecords.push(record);
            
            // Log first few transferred call additions for verification
            if (index < 3) {
              console.log(`📊 Added Transferred CDR record ${index + 1}: Call ID ${transferredCall.call_id || 'unknown'}, Extension: ${transferredCall.caller_id_number || 'unknown'}`);
              console.log(`   - Time difference: ${transferredCall.time_difference || 'unknown'} seconds`);
              console.log(`   - Transfer time: ${new Date((transferredCall.transfer_time || 0) * 1000).toISOString()}`);
              console.log(`   - CDR timestamp: ${new Date((transferredCall.cdr_timestamp || 0) * 1000).toISOString()}`);
            }
          });
        } else {
          console.log(`📊 No transferred call records found for call ${transfer.callId} to extension ${transfer.transferInfo.transfer_extension}`);
        }
      }
    }
    
    // Debug final record counts before normalization
    console.log(`📊 Final allRecords count: ${allRecords.length}`);
    const recordTypeCounts = {};

    allRecords.forEach(record => {
      const type = record.record_type || record._recordType || 'Unknown';
      recordTypeCounts[type] = (recordTypeCounts[type] || 0) + 1;
    });
    console.log(`📊 Records by type:`, recordTypeCounts);
    
    // Follow-up notes are already extracted directly from API responses
    const normalizedRecords = allRecords;
    
    // Apply filters if provided in params
    let filteredRecords = normalizedRecords;
    if (params.filters && Object.keys(params.filters).length > 0) {
      console.log(`📊 Applying backend filters to ${normalizedRecords.length} records:`, params.filters);
      
      filteredRecords = normalizedRecords.filter(record => {
        // Check if record matches all filter criteria
        return Object.entries(params.filters).every(([columnName, filterValue]) => {
          // Skip empty filter values
          if (!filterValue || filterValue.trim() === '') return true;
          
          // Get the record value for this column
          const recordValue = record[columnName];
          if (recordValue === undefined || recordValue === null) return false;
          
          // Case-insensitive string comparison
          const recordValueStr = String(recordValue).toLowerCase();
          const filterValueStr = String(filterValue).toLowerCase();
          
          // Check if record value contains filter value
          return recordValueStr.includes(filterValueStr);
        });
      });
      
      console.log(`📊 Filter applied: ${normalizedRecords.length} -> ${filteredRecords.length} records`);
    }
    
    // Contact number filtering is now handled at the database level
    // This section is kept for backward compatibility with frontend filters
    if (params.contactNumber && params.contactNumber.trim() !== '' && !params._dbFiltered) {
      console.log(`📊 Warning: Contact number filtering should be handled at database level`);
      console.log(`📊 This is a fallback mechanism and may be less efficient`);
      
      // Normalize the search contact number
      const searchNumber = normalizePhoneNumber(params.contactNumber);
      
      if (searchNumber) {
        const beforeCount = filteredRecords.length;
        
        filteredRecords = filteredRecords.filter(record => {
          // Check various phone number fields that might contain the contact number
          const contactFields = [
            record['Contact number'],
            record['Caller ID Number'],
            record['Callee ID / Lead number'],
            record['lead_number'],
            record['caller_id_number'],
            record['callee_id_number']
          ];
          
          // Normalize each field and check if it contains the search number
          return contactFields.some(field => {
            if (!field) return false;
            const normalizedField = normalizePhoneNumber(String(field));
            return normalizedField.includes(searchNumber);
          });
        });
        
        console.log(`📊 Contact number filter applied: ${beforeCount} -> ${filteredRecords.length} records`);
      }
    }
    
    // Sort by timestamp (most recent first)
    filteredRecords.sort((a, b) => {
      const getTimestamp = (record) => {
        const timeStr = record['Called Time'];
        if (!timeStr) return 0;
        
        // Handle numeric timestamps
        if (typeof timeStr === 'number') {
          return timeStr > 10_000_000_000 ? timeStr : timeStr * 1000;
        }
        
        // Handle string timestamps
        if (typeof timeStr === 'string') {
          // Pure digits (epoch)
          if (/^\d+(\.\d+)?$/.test(timeStr)) {
            const num = Number(timeStr);
            return timeStr.length > 10 ? num : num * 1000;
          }
          // ISO string
          return Date.parse(timeStr) || 0;
        }
        
        return 0;
      };
      return getTimestamp(b) - getTimestamp(a);
    });
    
    // Enhanced Queue Extension Transfer Matching
    console.log('📊 Step: Processing enhanced queue extension transfer matching...');
    
    // Extract ALL calls with transfers to queue extensions (outbound, inbound, campaign)
    const callsWithQueueTransfers = filteredRecords.filter(r =>
      (r._recordType === 'outbound' || r._recordType === 'inbound' || r._recordType === 'campaign') &&
      r.transfer_event &&
      (isQueueExtension(r.transfer_queue_extension) || isQueueExtension(r.transfer_extension))
    );
    
    console.log(`📊 ENHANCED ANALYSIS: Found ${callsWithQueueTransfers.length} calls with queue extension transfers (${callsWithQueueTransfers.filter(r => r._recordType === 'outbound').length} outbound, ${callsWithQueueTransfers.filter(r => r._recordType === 'inbound').length} inbound, ${callsWithQueueTransfers.filter(r => r._recordType === 'campaign').length} campaign)`);
    
    if (callsWithQueueTransfers.length > 0) {
      // Get inbound calls for matching
      const inboundCallsForMatching = filteredRecords.filter(record => record._recordType === 'inbound');
      
      console.log(`📊 INBOUND ANALYSIS: Found ${inboundCallsForMatching.length} inbound calls for matching`);
      
      // Enhanced logging for inbound calls with callee_id analysis
      const calleeIdCounts = {};
      inboundCallsForMatching.forEach(call => {
        const calleeId = call.callee_id_number || 
          (call.raw_data && typeof call.raw_data === 'object' ? call.raw_data.callee_id_number : null);
        if (calleeId) {
          calleeIdCounts[calleeId] = (calleeIdCounts[calleeId] || 0) + 1;
        }
      });
      
      console.log(`📊 CALLEE_ID DISTRIBUTION: ${Object.keys(calleeIdCounts).length} unique callee_id_numbers found`);
      Object.entries(calleeIdCounts).forEach(([calleeId, count]) => {
        console.log(`   ${calleeId}: ${count} calls`);
      });
      
      // Special check for 7014 (maps to queue extension 8001)
      const calls7014 = inboundCallsForMatching.filter(call => {
        const calleeId = call.callee_id_number || 
          (call.raw_data && typeof call.raw_data === 'object' ? call.raw_data.callee_id_number : null);
        return calleeId === '7014';
      });
      
      if (calls7014.length > 0) {
        console.log(`📊 SPECIAL CASE: Found ${calls7014.length} inbound calls with callee_id=7014 (for queue extension 8001)`);
        calls7014.forEach((call, idx) => {
          const callTime = call.called_time || call.timestamp;
          const callTimeMs = callTime < 10000000000 ? callTime * 1000 : callTime;
          console.log(`📊 7014 Call ${idx + 1}: ID ${call.callid || call.call_id}, Time: ${new Date(callTimeMs).toISOString()}`);
          
          // Check for agent information
          if (Array.isArray(call.agent_history) && call.agent_history.length > 0) {
            const firstAgent = call.agent_history[0];
            const agentExt = firstAgent.ext || 'N/A';
            const agentName = `${firstAgent.first_name || ''} ${firstAgent.last_name || ''}`.trim() || 'Unknown';
            console.log(`📊 7014 Agent: ${agentName} (Extension: ${agentExt})`);
          } else {
            const altExt = call.agent_answered_ext || call.agent_ext || call.extension || 'N/A';
            console.log(`📊 7014 Agent: Alternative extension = ${altExt}`);
          }
        });
      } else {
        console.log(`📊 SPECIAL CASE: No inbound calls found with callee_id=7014 (expected for queue extension 8001)`);
      }
      
      // Process enhanced matching for each call with queue extension transfer
      for (const call of callsWithQueueTransfers) {
        console.log(`📊 ENHANCED MATCHING: Processing ${call._recordType} call ${call.callid || call.call_id}`);
        
        // Find the appropriate timestamp for matching based on call type
        let matchingTimestamp = null;
        let historyArray = null;
        
        if (call._recordType === 'outbound' || call._recordType === 'inbound') {
          historyArray = call.agent_history;
          if (Array.isArray(historyArray)) {
            const lastHoldStartEvent = findLastHoldStartForQueueExtension(historyArray);
            if (lastHoldStartEvent) {
              matchingTimestamp = lastHoldStartEvent.last_attempt;
              console.log(`📊 HOLD START FOUND: Call ${call.callid || call.call_id} at ${new Date(lastHoldStartEvent.last_attempt * 1000).toISOString()}`);
            }
          }
        } else if (call._recordType === 'campaign') {
          historyArray = call.lead_history;
          if (Array.isArray(historyArray)) {
            // For campaign calls, use lead_answer timestamp
            const leadAnswerEvents = historyArray.filter(e => e && e.type === 'lead_answer');
            if (leadAnswerEvents.length > 0) {
              const lastLeadAnswer = leadAnswerEvents[leadAnswerEvents.length - 1];
              matchingTimestamp = lastLeadAnswer.last_attempt;
              console.log(`📊 LEAD ANSWER FOUND: Campaign call ${call.call_id} at ${new Date(lastLeadAnswer.last_attempt * 1000).toISOString()}`);
            }
          }
        }
        
        if (matchingTimestamp) {
          // Try to find a matching inbound call using the enhanced matching method
          let matchingCall = null;
          
          if (call._recordType === 'inbound') {
            // For inbound calls, find OTHER inbound calls with matching callee_id (exclude self)
            const queueExtension = call.transfer_queue_extension;
            const queueToCalleeExtensionMap = {
              '8000': '7020', '8001': '7014', '8002': '7015', '8003': '7016', '8004': '7012',
              '8005': '7017', '8006': '7018', '8007': '7019', '8008': '7021', '8009': '7034',
              '8010': '7023', '8011': '7028', '8012': '7029', '8013': '7031', '8014': '7033',
              '8015': '7030', '8016': '7013', '8017': '7011', '8018': '7010', '8019': '7008'
            };
            
            const expectedCalleeId = queueToCalleeExtensionMap[queueExtension];
            if (expectedCalleeId) {
              console.log(`📊 INBOUND-TO-INBOUND: Looking for other inbound calls with callee_id ${expectedCalleeId} for inbound call ${call.callid || call.call_id}`);
              
              // Find other inbound calls with matching callee_id (exclude self)
              const candidateCalls = inboundCallsForMatching.filter(inboundCall => {
                const calleeId = inboundCall.callee_id_number || 
                  (inboundCall.raw_data && typeof inboundCall.raw_data === 'object' ? inboundCall.raw_data.callee_id_number : null);
                const isSameCall = (inboundCall.callid || inboundCall.call_id) === (call.callid || call.call_id);
                return calleeId === expectedCalleeId && !isSameCall;
              });
              
              if (candidateCalls.length > 0) {
                // Find the closest match by time
                const callTime = call.called_time || call.timestamp;
                const callTimeMs = callTime < 10000000000 ? callTime * 1000 : callTime;
                
                candidateCalls.sort((a, b) => {
                  const aTime = a.called_time || a.timestamp;
                  const bTime = b.called_time || b.timestamp;
                  const aTimeMs = aTime < 10000000000 ? aTime * 1000 : aTime;
                  const bTimeMs = bTime < 10000000000 ? bTime * 1000 : bTime;
                  const aTimeDiff = Math.abs(aTimeMs - callTimeMs);
                  const bTimeDiff = Math.abs(bTimeMs - callTimeMs);
                  return aTimeDiff - bTimeDiff;
                });
                
                matchingCall = candidateCalls[0];
                console.log(`📊 INBOUND-TO-INBOUND MATCH: Found matching inbound call ${matchingCall.callid || matchingCall.call_id} for transfer from ${call.callid || call.call_id}`);
              } else {
                console.log(`📊 INBOUND-TO-INBOUND: No other inbound calls found with callee_id ${expectedCalleeId}`);
              }
            }
          } else {
            // For outbound and campaign calls, use the existing logic
            matchingCall = findMatchingInboundCallByHoldTime(
              call, { last_attempt: matchingTimestamp }, inboundCallsForMatching
            );
          }
            
          if (matchingCall) {
            console.log(`📊 ENHANCED MATCH FOUND: Inbound call ${matchingCall.callid || matchingCall.call_id} matches ${call._recordType} call ${call.callid || call.call_id}`);
              
              // Extract agent extension from the matching inbound call
              let agentExtension = null;
              
              // First try to get extension from agent_history
              if (Array.isArray(matchingCall.agent_history) && matchingCall.agent_history.length > 0) {
                for (const event of matchingCall.agent_history) {
                  if (event && event.ext) {
                    agentExtension = event.ext;
                    console.log(`📊 AGENT EXTENSION FOUND: ${agentExtension} from agent_history`);
                    break;
                  }
                }
              }
              
              // If we still don't have an agent extension, try other fields
              if (!agentExtension) {
                if (matchingCall.agent_answered_ext) {
                  agentExtension = matchingCall.agent_answered_ext;
                  console.log(`📊 AGENT EXTENSION FALLBACK: ${agentExtension} from agent_answered_ext`);
                } else if (matchingCall.agent_ext) {
                  agentExtension = matchingCall.agent_ext;
                  console.log(`📊 AGENT EXTENSION FALLBACK: ${agentExtension} from agent_ext`);
                } else if (matchingCall.extension) {
                  agentExtension = matchingCall.extension;
                  console.log(`📊 AGENT EXTENSION FALLBACK: ${agentExtension} from extension`);
                }
              }
              
              // Special case for 8001 -> 7014 mapping
              if (call.transfer_queue_extension === '8001' && !agentExtension) {
                agentExtension = '1002'; // Default for 8001 mapping
                console.log(`📊 SPECIAL CASE DEFAULT: Using agent extension 1002 for 8001->7014 mapping`);
              }
              
              if (agentExtension) {
                // Update the call with the matched agent extension
                call.transfer_extension = agentExtension;
                console.log(`📊 ENHANCED UPDATE: Set transfer_extension to ${agentExtension} for ${call._recordType} call ${call.callid || call.call_id}`);
              }
            } else {
              console.log(`📊 ENHANCED MATCHING: No matching inbound call found within time window for ${call._recordType} call ${call.callid || call.call_id}`);
              
              // Fallback: Try to find any inbound call with matching callee_id (outside time window)
              const queueExtension = call.transfer_queue_extension;
              const queueToCalleeExtensionMap = {
                '8000': '7020', '8001': '7014', '8002': '7015', '8003': '7016', '8004': '7012',
                '8005': '7017', '8006': '7018', '8007': '7019', '8008': '7021', '8009': '7034',
                '8010': '7023', '8011': '7028', '8012': '7029', '8013': '7031', '8014': '7033',
                '8015': '7030', '8016': '7013', '8017': '7011', '8018': '7010', '8019': '7008'
              };
              
              const expectedCalleeId = queueToCalleeExtensionMap[queueExtension];
              if (expectedCalleeId) {
                console.log(`📊 ENHANCED FALLBACK: Searching for any inbound call with callee_id ${expectedCalleeId}...`);
                
                const fallbackCalls = inboundCallsForMatching.filter(inboundCall => {
                  const calleeId = inboundCall.callee_id_number || 
                    (inboundCall.raw_data && typeof inboundCall.raw_data === 'object' ? inboundCall.raw_data.callee_id_number : null);
                  
                  // For inbound calls, exclude self-matching
                  if (call._recordType === 'inbound') {
                    const isSameCall = (inboundCall.callid || inboundCall.call_id) === (call.callid || call.call_id);
                    return calleeId === expectedCalleeId && !isSameCall;
                  }
                  
                  return calleeId === expectedCalleeId;
                });
                
                if (fallbackCalls.length > 0) {
                  // Sort by time difference to get the closest match (even if outside window)
                  fallbackCalls.sort((a, b) => {
                    const aTime = a.called_time || a.timestamp;
                    const bTime = b.called_time || b.timestamp;
                    const aTimeDiff = Math.abs((aTime < 10000000000 ? aTime * 1000 : aTime) - 
                      (matchingTimestamp < 10000000000 ? matchingTimestamp * 1000 : matchingTimestamp));
                    const bTimeDiff = Math.abs((bTime < 10000000000 ? bTime * 1000 : bTime) - 
                      (matchingTimestamp < 10000000000 ? matchingTimestamp * 1000 : matchingTimestamp));
                    return aTimeDiff - bTimeDiff;
                  });
                  
                  const fallbackCall = fallbackCalls[0];
                  const fallbackCallId = fallbackCall.callid || fallbackCall.call_id;
                  
                  console.log(`📊 ENHANCED FALLBACK MATCH: Using closest inbound call ${fallbackCallId} (outside time window)`);
                  
                  // Extract agent extension from fallback call
                  let agentExtension = null;
                  
                  if (Array.isArray(fallbackCall.agent_history) && fallbackCall.agent_history.length > 0) {
                    for (const event of fallbackCall.agent_history) {
                      if (event && event.ext) {
                        agentExtension = event.ext;
                        console.log(`📊 ENHANCED FALLBACK AGENT: ${agentExtension} from agent_history`);
                        break;
                      }
                    }
                  }
                  
                  if (!agentExtension) {
                    if (fallbackCall.agent_answered_ext) {
                      agentExtension = fallbackCall.agent_answered_ext;
                      console.log(`📊 ENHANCED FALLBACK AGENT: ${agentExtension} from agent_answered_ext`);
                    } else if (fallbackCall.agent_ext) {
                      agentExtension = fallbackCall.agent_ext;
                      console.log(`📊 ENHANCED FALLBACK AGENT: ${agentExtension} from agent_ext`);
                    } else if (fallbackCall.extension) {
                      agentExtension = fallbackCall.extension;
                      console.log(`📊 ENHANCED FALLBACK AGENT: ${agentExtension} from extension`);
                    }
                  }
                  
                  if (agentExtension) {
                    call.transfer_extension = agentExtension;
                    console.log(`📊 ENHANCED FALLBACK SET: transfer_extension=${agentExtension} for ${call._recordType} call ${call.callid || call.call_id}`);
                  }
                } else {
                  console.log(`📊 ENHANCED FALLBACK: No inbound calls found with callee_id ${expectedCalleeId} at all`);
                }
              }
            }
          } else {
            console.log(`📊 MATCHING TIMESTAMP: No matching timestamp found for ${call._recordType} call ${call.callid || call.call_id}`);
          }
        }
      }
    
    const cdrCount = allRecords.filter(r => r.record_type === 'CDR').length;
    const recordsWithFollowUp = allRecords.filter(r => r['Follow up notes'] && r['Follow up notes'].trim() !== '').length;
    
    console.log(`📊 Unified report created: ${allRecords.length} total records`);
    console.log(`   - Campaigns: ${parsedCampaignRecords.length}`);
    console.log(`   - Inbound: ${parsedInboundRecords.length}`);
    console.log(`   - Outbound: ${parsedOutboundRecords.length}`);
    
    return {
      rows: allRecords,
      summary: {
        total: allRecords.length,
        campaigns: parsedCampaignRecords.length,
        inbound: parsedInboundRecords.length,
        outbound: parsedOutboundRecords.length,
        cdr: cdrCount,
        recordsWithFollowUp
      }
    };
  } catch (error) {
    console.error('âŒ Error creating unified report from database:', error);
    throw error;
  }
}

/**
 * Enhanced CLI: node -r dotenv/config reportFetcher.js <report|all> <tenant> <startISO> <endISO> [outfile]
 * 
 * If 'all' is specified as the report type, all report types will be fetched and stored.
 */
async function cli() {
  const [,, report, tenant, startIso, endIso, outFile] = process.argv;
  if (!report || !tenant) {
    console.error('Usage: node -r dotenv/config reportFetcher.js <report|all> <tenant> [startISO] [endISO] [outfile.{csv|json}]');
    console.error(`report = ${Object.keys(ENDPOINTS).join(' | ')} | all`);
    process.exit(1);
  }
  const params = {};
  if (startIso) {
    const startDate = Date.parse(startIso);
    if (Number.isNaN(startDate)) throw new Error('Invalid start date');
    params.startDate = Math.floor(startDate / 1000);
  }
  if (endIso) {
    const endDate = Date.parse(endIso);
    if (Number.isNaN(endDate)) throw new Error('Invalid end date');
    params.endDate = Math.floor(endDate / 1000);
  }

  // If 'all' is specified, fetch and store data from all APIs
  if (report === 'all') {
    console.log(' 📊 Fetching and storing data from all APIs...');
    const results = {};
    
    // Fetch and store data from each API
    for (const reportType of Object.keys(ENDPOINTS)) {
      console.log(`\n 📊 Processing ${reportType} API...`);
      try {
        const data = await fetchReport(reportType, tenant, params);
        results[reportType] = data.rows.length;
        console.log(` 📊 Successfully fetched and stored ${data.rows.length} records for ${reportType}`);
      } catch (error) {
        console.error(` ❌ Error fetching/storing ${reportType} data:`, error.message);
        results[reportType] = 'ERROR';
      }
    }
    
    console.log('\n 📊 Summary of fetched and stored records:');
    console.table(results);
    
    if (outFile) {
      await fs.promises.mkdir(path.dirname(outFile), { recursive: true });
      await fs.promises.writeFile(
        outFile, 
        JSON.stringify(results, null, 2)
      );
      console.log(` 📊 Summary saved to ${outFile}`);
    }
  } else {
    // Original behavior for a single report type
    if (!ENDPOINTS[report]) {
      console.error(`Unknown report type: ${report}`);
      console.error(`Available report types: ${Object.keys(ENDPOINTS).join(', ')} or 'all'`);
      process.exit(1);
    }
    
    const data = await fetchReport(report, tenant, params);
    console.log(`Fetched ${data.rows.length} rows for ${report}`);

    if (outFile) {
      await fs.promises.mkdir(path.dirname(outFile), { recursive: true });
      if (outFile.endsWith('.csv')) {
        await fs.promises.writeFile(outFile, toCsv(data.rows));
      } else {
        await fs.promises.writeFile(outFile, JSON.stringify(data.rows, null, 2));
      }
      console.log(`Saved to ${outFile}`);
    } else {
      console.table(data.rows);
    }
  }
}

// Execute CLI if run directly
const __filename = fileURLToPath(import.meta.url);
if (process.argv[1] === __filename) {
  cli().catch(err => {
    console.error('❌ Unhandled error in CLI:', err);
    process.exit(1);
  });
}
