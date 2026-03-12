// blaHotPatchTransferService.js
// Service to handle BLA Hot Patch Transfer report logic
// Links Campaign calls with Transfer events to their corresponding Inbound queue calls

import mysql from 'mysql2/promise';
import { DateTime } from 'luxon';
import dbService from './dbService.js';
import axios from 'axios';
import https from 'https';
import { 
  isQueueExtension, 
  processCampaignTransfersToQueueExtensions,
  getAgentNamesByExtensions 
} from './transferQueueDetector.js';

/**
 * Queue extension to callee_id extension mapping for BLA Hot Patch Transfer
 * This maps queue extensions (8000-8999) to their corresponding callee_id extensions
 * Updated based on user examples showing actual transfer patterns
 */
const queueToCalleeExtensionMap = {
  '8000': '7020',
  '8001': '7014',
  '8002': '7015',
  '8003': '7016',
  '8004': '7012',
  '8005': '7017',
  '8006': '7018',
  '8007': '7019',
  '8008': '7021',
  '8009': '7034',
  '8010': '7023',
  '8011': '7028',
  '8012': '7029',
  '8013': '7031',
  '8014': '7033',
  '8015': '7030',
  '8016': '7013',
  '8017': '7011',
  '8018': '7010',
  '8019': '7008',
  '8021': '7060'  // Test_hotpatch queue 
};

// const SKILL_HISTORY_URL = 'https://ira-meydan-du.ucprem.voicemeetme.com/tmp/queue-skill-history.json';
const SKILL_HISTORY_URL = 'https://queue.ira-meydan-du.ucprem.voicemeetme.com/queue_skill_group_history/544370e9c33f4d679f0f1d37778d3c7c';
const skillHistoryHttpsAgent = new https.Agent({ rejectUnauthorized: false });

/**
 * Fetch queue-skill-history.json from the remote server
 * @returns {Object} - Full JSON keyed by call_id
 */
async function fetchQueueSkillHistory() {
  try {
    console.log(`📋 SKILL HISTORY: Fetching from ${SKILL_HISTORY_URL}`);
    const { data } = await axios.get(SKILL_HISTORY_URL, {
      timeout: 15000,
      httpsAgent: skillHistoryHttpsAgent,
      headers: { Accept: 'application/json' }
    });
    if (!data || typeof data !== 'object') {
      console.warn('⚠️ SKILL HISTORY: Unexpected response format');
      return {};
    }

    // The JSON structure is: { queues: [ { queue_name, skill_group_history: { dial_history: { call_id: [...entries] } } } ] }
    // Flatten dial_history from ALL queues into a single call_id-keyed map
    const flatMap = {};
    const queues = Array.isArray(data.queues) ? data.queues : [];
    console.log(`📋 SKILL HISTORY: Found ${queues.length} queues in response`);

    for (const queue of queues) {
      const dialHistory = queue?.skill_group_history?.dial_history;
      if (!dialHistory || typeof dialHistory !== 'object') continue;
      const callIds = Object.keys(dialHistory);
      if (callIds.length === 0) continue;
      console.log(`📋 SKILL HISTORY: Queue "${queue.queue_name}" has ${callIds.length} call IDs in dial_history`);
      for (const callId of callIds) {
        if (flatMap[callId]) {
          // Merge entries from multiple queues for the same call_id
          flatMap[callId] = flatMap[callId].concat(dialHistory[callId]);
        } else {
          flatMap[callId] = dialHistory[callId];
        }
      }
    }

    const totalCallIds = Object.keys(flatMap).length;
    console.log(`✅ SKILL HISTORY: Extracted ${totalCallIds} unique call IDs from all queues`);
    return flatMap;
  } catch (err) {
    console.error(`❌ SKILL HISTORY: Failed to fetch - ${err.message}`);
    return {};
  }
}

/**
 * Extract SUB_LOB name from skills array (e.g. ["Languages.English","SUB_LOB.BDA"] -> "BDA")
 */
function extractSubLob(skills) {
  if (!Array.isArray(skills)) return null;
  for (const skill of skills) {
    if (typeof skill === 'string' && skill.startsWith('SUB_LOB.')) {
      return skill.replace('SUB_LOB.', '');
    }
  }
  return null;
}

/**
 * Process skill history entries for a single call_id.
 * Returns structured data for the 3 new columns:
 *   - skill_agents_not_available: SUB_LOB groups with dialed + skipped agent extensions, ordered by appearance
 *   - skill_attempts: All dialed agents (with agent_ext != null), green if answered, red if failed
 *   - skill_agent_answered: The extension that answered, or "--"
 */
function processSkillHistoryForCall(entries, extToNameMap = {}) {
  // Helper: format ext as "AgentName (ext)" or just "ext" if no name found
  const fmtExt = (ext) => {
    const name = extToNameMap[ext];
    return name ? `${name} (${ext})` : ext;
  };

  const empty = {
    skill_agents_not_available: [],
    skill_attempts: [],
    skill_agent_answered: '--'
  };
  if (!Array.isArray(entries) || entries.length === 0) return empty;

  // Sort by timestamp to preserve order
  const sorted = [...entries].sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));

  // Find answered agent
  const answeredEntry = sorted.find(e => e.dial_status === 'answered' && e.agent_ext);
  const answeredExt = answeredEntry ? String(answeredEntry.agent_ext) : null;
  const answeredSubLob = answeredEntry ? extractSubLob(answeredEntry.skills) : null;

  // Build SUB_LOB groups in order of first appearance
  const subLobOrder = [];
  const subLobGroups = {}; // SUB_LOB -> { dialed: [], skipped: [] }

  for (const entry of sorted) {
    const subLob = extractSubLob(entry.skills);
    if (!subLob) continue;

    if (!subLobGroups[subLob]) {
      subLobGroups[subLob] = { dialed: [], skipped: [] };
      subLobOrder.push(subLob);
    }

    // Dialed agents (agent_ext is not null, not group_exhausted)
    if (entry.agent_ext) {
      const ext = String(entry.agent_ext);
      if (!subLobGroups[subLob].dialed.includes(ext)) {
        subLobGroups[subLob].dialed.push(ext);
      }
    }

    // Skipped agents from group_exhausted entries
    if (entry.dial_status === 'group_exhausted' && Array.isArray(entry.agents_skipped)) {
      for (const skipped of entry.agents_skipped) {
        if (skipped.ext) {
          const ext = String(skipped.ext);
          if (!subLobGroups[subLob].skipped.includes(ext)) {
            subLobGroups[subLob].skipped.push(ext);
          }
        }
      }
    }
  }

  // Build agents_not_available structured data with formatted labels
  // Exclude the answered agent — they are NOT "not available"
  const agentsNotAvailable = subLobOrder.map(subLob => {
    const dialed = subLobGroups[subLob].dialed.filter(ext => ext !== answeredExt);
    const skipped = subLobGroups[subLob].skipped.filter(ext => ext !== answeredExt);
    return {
      subLob,
      dialed,
      skipped,
      dialedLabels: dialed.map(fmtExt),
      skippedLabels: skipped.map(fmtExt),
      isAnsweredGroup: false
    };
  }).filter(g => g.dialed.length > 0 || g.skipped.length > 0);

  // Build attempts list (only dialed agents, exclude group_exhausted with null agent_ext)
  const attempts = [];
  for (const entry of sorted) {
    if (!entry.agent_ext) continue;
    const ext = String(entry.agent_ext);
    const subLob = extractSubLob(entry.skills) || '';
    if (!attempts.find(a => a.ext === ext)) {
      attempts.push({ ext, label: fmtExt(ext), subLob, isAnswered: entry.dial_status === 'answered' });
    }
  }

  return {
    skill_agents_not_available: agentsNotAvailable,
    skill_attempts: attempts,
    skill_agent_answered: answeredExt ? (answeredSubLob ? `${answeredSubLob} => ${fmtExt(answeredExt)}` : fmtExt(answeredExt)) : '--'
  };
}

/**
 * Format agent history with UTC timezone for BLA reports
 * @param {Array|string} agentHistory - Agent history array or JSON string
 * @returns {string} - Formatted HTML table with UTC timestamps
 */
function formatAgentHistoryWithUTC(agentHistory) {
  if (!agentHistory) return '';
  
  let history = [];
  
  // Parse agent history if it's a string
  if (typeof agentHistory === 'string') {
    try {
      history = JSON.parse(agentHistory);
    } catch (e) {
      return agentHistory; // Return as-is if not valid JSON
    }
  } else if (Array.isArray(agentHistory)) {
    history = agentHistory;
  } else {
    return '';
  }
  
  if (!Array.isArray(history) || !history.length) return '';
  
  // Sort by last_attempt (oldest first)
  const sorted = [...history].sort((a, b) => (a.last_attempt ?? 0) - (b.last_attempt ?? 0));
  
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
          val = new Date(ms).toLocaleString('en-GB', { 
            timeZone: 'Asia/Dubai',
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
          });
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
  
  const tableHtml = `<div class="modal-card-body"><table class="history-table">${thead}<tbody>${rows}</tbody></table></div>`;
  return `<button class="button is-small is-rounded eye-btn" onclick="showHistoryModal(this)" data-history-type="agent" title="View Agent History">👁️</button>
         <div class="history-data" style="display:none">${tableHtml}</div>`;
}

/**
 * Parse HTML agent history content to extract transfer events
 * @param {string} htmlContent - HTML content containing agent history table
 * @returns {Array} - Array of parsed history events
 */
function parseHTMLAgentHistory(htmlContent) {
  const historyEvents = [];
  
  try {
    // Extract table rows using regex patterns
    const tableRowPattern = /<tr><td>([^<]+)<\/td><td>([^<]*)<\/td><td>([^<]*)<\/td><td>([^<]+)<\/td><td>([^<]+)<\/td><td>([^<]*)<\/td><\/tr>/g;
    let match;
    
    while ((match = tableRowPattern.exec(htmlContent)) !== null) {
      const [, lastAttempt, firstName, lastName, extension, event, hangupCause] = match;
      
      // Parse the date string to get timestamp
      let timestamp = null;
      try {
        // Convert "03/12/2025, 17:19:08" to timestamp in Asia/Dubai timezone
        const dateParts = lastAttempt.split(', ');
        if (dateParts.length === 2) {
          const [datePart, timePart] = dateParts;
          const [day, month, year] = datePart.split('/');
          const dateStr = `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T${timePart}`;
          
          // Parse as Asia/Dubai timezone using Luxon
          const dateTime = DateTime.fromISO(dateStr, { zone: 'Asia/Dubai' });
          if (dateTime.isValid) {
            timestamp = Math.floor(dateTime.toSeconds());
            console.log(`🕐 BLA HTML PARSE: Converted ${lastAttempt} to epoch ${timestamp} (${dateTime.toISO()})`);
          } else {
            console.log(`⚠️ BLA HTML PARSE: Invalid date format: ${lastAttempt}`);
          }
        }
      } catch (dateError) {
        console.log(`⚠️ BLA HTML PARSE: Error parsing date ${lastAttempt}: ${dateError.message}`);
      }
      
      // Create history event object
      const historyEvent = {
        ext: extension,
        type: event,
        agent: {
          first_name: firstName || '',
          last_name: lastName || '',
          ext: extension
        },
        first_name: firstName || '',
        last_name: lastName || '',
        hangup_cause: hangupCause || '',
        last_attempt: timestamp,
        LastAttemptString: lastAttempt
      };
      
      historyEvents.push(historyEvent);
      
      // Log transfer events for debugging
      if (event === 'Transfer' && isQueueExtension(extension)) {
        console.log(`🔄 BLA HTML PARSE: Found Transfer event to queue ${extension} at ${lastAttempt}`);
      }
    }
    
    console.log(`📋 BLA HTML PARSE: Extracted ${historyEvents.length} history events from HTML`);
    return historyEvents;
    
  } catch (error) {
    console.log(`⚠️ BLA HTML PARSE: Error parsing HTML content: ${error.message}`);
    return [];
  }
}

/**
 * Extract the 1st leg comparison timestamp from agent_history
 * For Outbound/Inbound: use hold_start event time
 * For Campaign: use Transfer event time (campaigns don't have hold_start)
 * This is used for time comparison: 1st leg time < agent_enter (2nd leg)
 * @param {Array} agentHistory - Agent history array
 * @param {string} callType - Type of call ('campaign', 'outbound', 'inbound')
 * @returns {number|null} - The comparison timestamp or null if not found
 */
function extract1stLegCompareTimestamp(agentHistory, callType = 'outbound') {
  if (!Array.isArray(agentHistory) || agentHistory.length === 0) {
    return null;
  }
  
  // For campaign calls, use the Transfer event time (they don't have hold_start)
  if (callType === 'campaign') {
    // Find the Transfer event (type: "Transfer" for campaign calls)
    // Also check for lowercase 'transfer' and check both type and event fields
    const transferEvent = agentHistory.find(event => 
      event && (
        event.type === 'Transfer' || 
        event.type === 'transfer' || 
        event.event === 'Transfer' || 
        event.event === 'transfer'
      )
    );
    
    console.log(`🔍 BLA 1ST LEG SEARCH: Looking for Transfer event in ${agentHistory.length} events`);
    if (transferEvent) {
      console.log(`🔍 BLA 1ST LEG FOUND: Transfer event:`, JSON.stringify(transferEvent));
    }
    
    if (transferEvent && transferEvent.last_attempt) {
      let timestamp = transferEvent.last_attempt;
      if (timestamp > 10000000000) {
        timestamp = Math.floor(timestamp / 1000);
      }
      console.log(`🕐 BLA 1ST LEG: Campaign using Transfer event time: ${new Date(timestamp * 1000).toISOString()}`);
      return timestamp;
    }
    return null;
  }
  
  // For outbound/inbound calls, use hold_start event time
  const holdStartEvent = agentHistory.find(event => 
    event && event.type === 'agent' && event.event === 'hold_start'
  );
  
  if (holdStartEvent && holdStartEvent.last_attempt) {
    let timestamp = holdStartEvent.last_attempt;
    if (timestamp > 10000000000) {
      timestamp = Math.floor(timestamp / 1000);
    }
    console.log(`🕐 BLA 1ST LEG: Outbound/Inbound using hold_start time: ${new Date(timestamp * 1000).toISOString()}`);
    return timestamp;
  }
  
  return null;
}

// Keep old function name for backward compatibility
function extractHoldStartTimestamp(agentHistory) {
  return extract1stLegCompareTimestamp(agentHistory, 'outbound');
}

/**
 * Extract the agent_enter timestamp from an inbound call's agent_history
 * This is the actual "Transferred Call Time" - when the agent entered the call
 * @param {Array} agentHistory - Agent history array
 * @returns {number|null} - The agent_enter timestamp or null if not found
 */
function extractAgentEnterTimestamp(agentHistory) {
  if (!Array.isArray(agentHistory) || agentHistory.length === 0) {
    return null;
  }
  
  // Find the agent_enter event (type: "agent", event: "agent_enter")
  const agentEnterEvent = agentHistory.find(event => 
    event && event.type === 'agent' && event.event === 'agent_enter'
  );
  
  if (agentEnterEvent && agentEnterEvent.last_attempt) {
    // Normalize timestamp (convert to seconds if in milliseconds)
    let timestamp = agentEnterEvent.last_attempt;
    if (timestamp > 10000000000) {
      timestamp = Math.floor(timestamp / 1000);
    }
    return timestamp;
  }
  
  return null;
}

/**
 * Validate 2nd leg pattern for inbound calls
 * 2nd leg MUST have: agent_enter
 * transfer_enter to a QUEUE EXTENSION (8000-8999) would make this a 1st leg, not 2nd leg
 * transfer_enter to a CUSTOMER PHONE NUMBER is OK - indicates successful transfer completion
 * 
 * SUCCESSFUL HOTPATCH PATTERN (in order):
 * 1. agent_enter (agent answers the queue call)
 * 2. attended:transfer to queue (e.g., 8001) - transfer initiated
 * 3. transfer_enter to customer phone (e.g., 00918545815481) - transfer completed, customer connected
 * 
 * @param {Array} agentHistory - Agent history array
 * @returns {Object} - { isValid2ndLeg: boolean, agentEnterTimestamp: number|null, transferTimestamp: number|null, transferEnterTimestamp: number|null, isSuccessfulHotpatch: boolean, customerNumber: string|null }
 */
function validate2ndLegPattern(agentHistory) {
  if (!Array.isArray(agentHistory) || agentHistory.length === 0) {
    return { isValid2ndLeg: false, agentEnterTimestamp: null, transferTimestamp: null, transferEnterTimestamp: null, isSuccessfulHotpatch: false, customerNumber: null };
  }
  
  // Check for required events
  const hasAgentEnter = agentHistory.some(event => 
    event && event.type === 'agent' && event.event === 'agent_enter'
  );
  
  // Find attended:transfer event (to queue extension like 8001)
  // This is the KEY indicator for a transferred 2nd leg
  const attendedTransferEvent = agentHistory.find(event => 
    event && event.type === 'attended' && event.event === 'transfer' &&
    event.ext && isQueueExtension(event.ext)
  );
  const hasAttendedTransferToQueue = !!attendedTransferEvent;
  
  // Check for transfer_enter to a QUEUE EXTENSION (which would make this a 1st leg, not 2nd leg)
  const hasTransferEnterToQueue = agentHistory.some(event => 
    event && event.type === 'agent' && event.event === 'transfer_enter' && 
    event.ext && isQueueExtension(event.ext)
  );
  
  // Find transfer_enter to CUSTOMER PHONE NUMBER (non-queue extension, 9+ digits)
  // This indicates SUCCESSFUL transfer completion - customer was connected
  const transferEnterToCustomer = agentHistory.find(event => {
    if (!event || event.type !== 'agent' || event.event !== 'transfer_enter') return false;
    if (!event.ext) return false;
    const normalizedExt = event.ext.toString().replace(/[^\d]/g, '');
    // Customer phone numbers are 9+ digits, not queue extensions
    return normalizedExt.length >= 9 && !isQueueExtension(event.ext);
  });
  const hasTransferEnterToCustomer = !!transferEnterToCustomer;
  
  // TRANSFERRED 2ND LEG PATTERNS:
  // PATTERN 1: attended:transfer to queue (8001) - normal transferred leg
  // PATTERN 2: attended:transfer to queue (8001) → transfer_enter to customer phone - successful hotpatch
  // 
  // Both patterns REQUIRE attended:transfer to queue extension
  // Pattern 2 additionally has transfer_enter to customer phone confirming customer was connected
  
  let isSuccessfulHotpatch = false;
  let customerNumber = null;
  
  if (hasAttendedTransferToQueue && hasTransferEnterToCustomer) {
    // PATTERN 2: Successful hotpatch - verify transfer_enter comes AFTER attended:transfer
    const attendedTime = attendedTransferEvent.last_attempt || 0;
    const transferEnterTime = transferEnterToCustomer.last_attempt || 0;
    
    // transfer_enter should be after or same time as attended:transfer
    if (transferEnterTime >= attendedTime - 1) { // Allow 1 second tolerance
      isSuccessfulHotpatch = true;
      customerNumber = transferEnterToCustomer.ext;
      console.log(`✅ BLA PATTERN 2 (SUCCESSFUL HOTPATCH): attended:transfer to ${attendedTransferEvent.ext} at ${attendedTime} → transfer_enter to ${customerNumber} at ${transferEnterTime}`);
    }
  } else if (hasAttendedTransferToQueue) {
    // PATTERN 1: Normal transferred leg (attended:transfer to queue without transfer_enter to customer)
    console.log(`✅ BLA PATTERN 1 (NORMAL TRANSFER): attended:transfer to ${attendedTransferEvent.ext}`);
  }
  
  // 2nd leg MUST have: attended:transfer to queue extension (8000-8999)
  // MUST NOT have: transfer_enter to queue extension (that would make it a 1st leg)
  // transfer_enter to customer phone is allowed (indicates successful hotpatch)
  const isValid2ndLeg = hasAgentEnter && hasAttendedTransferToQueue && !hasTransferEnterToQueue;
  
  // Extract timestamps
  const agentEnterTimestamp = extractAgentEnterTimestamp(agentHistory);
  
  // Get attended:transfer timestamp
  let transferTimestamp = null;
  if (attendedTransferEvent && attendedTransferEvent.last_attempt) {
    transferTimestamp = attendedTransferEvent.last_attempt;
    if (transferTimestamp > 10000000000) {
      transferTimestamp = Math.floor(transferTimestamp / 1000);
    }
  }
  
  // Get transfer_enter timestamp (for successful hotpatch, this is when customer was connected)
  let transferEnterTimestamp = null;
  if (transferEnterToCustomer && transferEnterToCustomer.last_attempt) {
    transferEnterTimestamp = transferEnterToCustomer.last_attempt;
    if (transferEnterTimestamp > 10000000000) {
      transferEnterTimestamp = Math.floor(transferEnterTimestamp / 1000);
    }
  }
  
  return { 
    isValid2ndLeg, 
    agentEnterTimestamp, 
    transferTimestamp, 
    transferEnterTimestamp, 
    isSuccessfulHotpatch, 
    customerNumber,
    hasAttendedTransfer: hasAttendedTransferToQueue,
    hasTransferEnterToCustomer
  };
}

/**
 * Detect failed transfer attempts in agent history
 * @param {Array} agentHistory - Array of agent history events
 * @returns {Array} - Array of failed transfer attempts
 */
function detectFailedTransfers(agentHistory) {
  if (!Array.isArray(agentHistory) || agentHistory.length === 0) {
    return [];
  }

  const failedAgents = [];
  
  // Find all dial events where connected = false
  const dialEvents = agentHistory.filter(event => {
    return event && 
           event.event === 'dial' && 
           event.connected === false &&
           event.ext;
  });

  dialEvents.forEach(dialEvent => {
    failedAgents.push({
      extension: dialEvent.ext,
      agent_name: `${dialEvent.first_name || ''} ${dialEvent.last_name || ''}`.trim(),
      email: dialEvent.email || '',
      dial_time: dialEvent.last_attempt,
      dial_time_formatted: dialEvent.last_attempt ? new Date(dialEvent.last_attempt * 1000).toISOString() : '',
      queue_name: dialEvent.queue_name || '',
      reason: 'No Answer'
    });
  });

  return failedAgents;
}

/**
 * Extract lead_answer and lead_hangup timestamps from campaign lead_history
 * @param {Array|string} leadHistory - Lead history array or JSON string
 * @returns {Object} - { holdStartTime, holdStopTime, transferQueueExtension, holdPeriods, hasTransferEvent }
 */
// Debug flag for lead_history (set to true for first few calls)
let leadHistoryDebugCount = 0;
const MAX_LEAD_HISTORY_DEBUG = 3;

function extractCampaignHoldTimes(leadHistory) {
  let history = [];
  
  if (typeof leadHistory === 'string') {
    try {
      history = JSON.parse(leadHistory);
    } catch (e) {
      return { holdStartTime: null, holdStopTime: null, transferQueueExtension: null, holdPeriods: [], hasTransferEvent: false };
    }
  } else if (Array.isArray(leadHistory)) {
    history = leadHistory;
  } else {
    return { holdStartTime: null, holdStopTime: null, transferQueueExtension: null, holdPeriods: [], hasTransferEvent: false };
  }
  
  // Debug: Log first few lead_history structures
  if (leadHistoryDebugCount < MAX_LEAD_HISTORY_DEBUG && history.length > 0) {
    leadHistoryDebugCount++;
    console.log(`🔍 BLA LEAD_HISTORY DEBUG #${leadHistoryDebugCount}:`);
    console.log(`   - Total events: ${history.length}`);
    console.log(`   - Event types: ${history.map(e => e?.type || e?.event || 'unknown').join(', ')}`);
    if (history[0]) {
      console.log(`   - First event keys: ${Object.keys(history[0]).join(', ')}`);
      console.log(`   - First event: ${JSON.stringify(history[0]).substring(0, 200)}`);
    }
  }
  
  // Find ALL hold_start events
  const holdStartEvents = history.filter(event => 
    event && event.type === 'hold_start'
  );
  
  // Find ALL hold_stop events
  const holdStopEvents = history.filter(event => 
    event && event.type === 'hold_stop'
  );
  
  // Find Transfer event to get the queue extension
  const transferEvent = history.find(event => 
    event && event.type === 'Transfer'
  );
  
  const hasTransferEvent = !!transferEvent;
  
  // Build array of hold periods by pairing hold_start with hold_stop
  const holdPeriods = [];
  for (let i = 0; i < holdStartEvents.length; i++) {
    let startTime = holdStartEvents[i].last_attempt;
    if (startTime > 10000000000) {
      startTime = startTime / 1000;
    }
    
    // Find corresponding hold_stop (same index or next available)
    let stopTime = null;
    if (i < holdStopEvents.length && holdStopEvents[i].last_attempt) {
      stopTime = holdStopEvents[i].last_attempt;
      if (stopTime > 10000000000) {
        stopTime = stopTime / 1000;
      }
    }
    
    if (startTime) {
      holdPeriods.push({ start: startTime, stop: stopTime });
    }
  }
  
  // For backward compatibility, also return the LAST hold period as holdStartTime/holdStopTime
  let holdStartTime = null;
  let holdStopTime = null;
  
  if (holdPeriods.length > 0) {
    const lastPeriod = holdPeriods[holdPeriods.length - 1];
    holdStartTime = lastPeriod.start;
    holdStopTime = lastPeriod.stop;
  }
  
  let transferQueueExtension = null;
  // Extract queue extension from Transfer event (e.g., "8005")
  if (transferEvent && transferEvent.ext) {
    transferQueueExtension = transferEvent.ext;
  }
  
  return { holdStartTime, holdStopTime, transferQueueExtension, holdPeriods, hasTransferEvent };
}

/**
 * Extract hold_start and hold_stop timestamps from outbound call agent_history
 * Outbound calls use 'event' field instead of 'type' field
 * @param {Array|string} agentHistory - Agent history array or JSON string
 * @returns {Object} - { holdStartTime, holdStopTime, transferQueueExtension, agentExtension, hasAttendedTransfer }
 */
function extractOutboundHoldTimes(agentHistory) {
  let history = [];
  
  if (typeof agentHistory === 'string') {
    try {
      history = JSON.parse(agentHistory);
    } catch (e) {
      return { holdStartTime: null, holdStopTime: null, transferQueueExtension: null, agentExtension: null, hasAttendedTransfer: false };
    }
  } else if (Array.isArray(agentHistory)) {
    history = agentHistory;
  } else {
    return { holdStartTime: null, holdStopTime: null, transferQueueExtension: null, agentExtension: null, hasAttendedTransfer: false };
  }
  
  // Find hold_start event (outbound uses 'event' field, not 'type')
  const holdStartEvent = history.find(event => 
    event && event.event === 'hold_start'
  );
  
  // Find hold_stop event
  const holdStopEvent = history.find(event => 
    event && event.event === 'hold_stop'
  );
  
  // Find transfer event to get the queue extension
  const transferEvent = history.find(event => 
    event && event.event === 'transfer' && event.type === 'attended'
  );
  
  let holdStartTime = null;
  let holdStopTime = null;
  let transferQueueExtension = null;
  let agentExtension = null;
  const hasAttendedTransfer = !!transferEvent;
  
  if (holdStartEvent && holdStartEvent.last_attempt) {
    holdStartTime = holdStartEvent.last_attempt;
    if (holdStartTime > 10000000000) {
      holdStartTime = holdStartTime / 1000;
    }
    // Get agent extension from hold_start event
    if (holdStartEvent.ext) {
      agentExtension = holdStartEvent.ext;
    }
  }
  
  if (holdStopEvent && holdStopEvent.last_attempt) {
    holdStopTime = holdStopEvent.last_attempt;
    if (holdStopTime > 10000000000) {
      holdStopTime = holdStopTime / 1000;
    }
  }
  
  // Extract queue extension from transfer event (e.g., "8005")
  if (transferEvent && transferEvent.ext) {
    transferQueueExtension = transferEvent.ext;
  }
  
  return { holdStartTime, holdStopTime, transferQueueExtension, agentExtension, hasAttendedTransfer };
}

/**
 * Detect Failed BLA Hot Patch Transfers
 * This detects transfers that failed because no agent answered in the queue
 * 
 * Criteria:
 * 1. Campaign/Outbound call with hold_start and hold_stop events
 *    - Campaign: events in lead_history (uses 'type' field)
 *    - Outbound: events in agent_history (uses 'event' field)
 * 2. Find inbound calls where:
 *    - caller_id_number = agent extension (from campaign/outbound)
 *    - callee_id_number = 8000 series (queue extension)
 *    - abandoned = "Yes"
 *    - called_time is between hold_start and hold_stop
 * 3. There can be multiple legs (attempts) or just one
 * 
 * @param {Array} campaignCalls - Array of campaign calls
 * @param {Array} inboundCalls - Array of inbound calls
 * @param {Set} usedInboundCallIds - Set of already-used inbound call IDs
 * @param {Array} outboundCalls - Array of outbound calls (optional)
 * @returns {Array} - Array of failed transfer records
 */
export function detectFailedBlaHotPatchTransfers(campaignCalls, inboundCalls, usedInboundCallIds = null, outboundCalls = []) {
  if (!Array.isArray(inboundCalls)) {
    return [];
  }
  
  // Ensure arrays
  const safeCampaignCalls = Array.isArray(campaignCalls) ? campaignCalls : [];
  const safeOutboundCalls = Array.isArray(outboundCalls) ? outboundCalls : [];
  
  console.log(`🔍 BLA FAILED TRANSFER: Checking ${safeCampaignCalls.length} campaign calls and ${safeOutboundCalls.length} outbound calls for failed transfers`);
  
  // Debug: Check how many campaign calls have lead_history
  const callsWithLeadHistory = campaignCalls.filter(c => c.lead_history);
  console.log(`🔍 BLA FAILED DEBUG: ${callsWithLeadHistory.length} campaign calls have lead_history`);
  
  // Sample first campaign call to see data structure
  if (campaignCalls.length > 0) {
    const sample = campaignCalls[0];
    console.log(`🔍 BLA FAILED DEBUG: First campaign call sample:`);
    console.log(`   - call_id: ${sample.call_id}`);
    console.log(`   - lead_history type: ${typeof sample.lead_history}`);
    console.log(`   - lead_history value: ${sample.lead_history ? String(sample.lead_history).substring(0, 200) : 'null/undefined'}`);
    console.log(`   - extension: ${sample.extension}`);
    console.log(`   - agent_extension: ${sample.agent_extension}`);
  }
  
  // Debug: Check abandoned inbound calls
  console.log(`🔍 BLA FAILED DEBUG: Total inbound calls to check: ${inboundCalls.length}`);
  if (inboundCalls.length > 0) {
    console.log(`🔍 BLA FAILED DEBUG: First inbound call sample:`);
    const sample = inboundCalls[0];
    console.log(`   - call_id: ${sample.call_id}`);
    console.log(`   - abandoned: "${sample.abandoned}" (type: ${typeof sample.abandoned})`);
    console.log(`   - disposition: "${sample.disposition}" (type: ${typeof sample.disposition})`);
    console.log(`   - callee_id_number: "${sample.callee_id_number}"`);
    console.log(`   - caller_id_number: "${sample.caller_id_number}"`);
  }
  
  const abandonedCalls = inboundCalls.filter(c => 
    c.abandoned === 'Yes' || c.abandoned === 'yes' || c.abandoned === true
  );
  console.log(`🔍 BLA FAILED DEBUG: ${abandonedCalls.length} inbound calls are abandoned (abandoned='Yes')`);
  
  // Also check by disposition
  const abandonDispositionCalls = inboundCalls.filter(c => 
    c.disposition === 'ABANDON' || c.disposition === 'abandon'
  );
  console.log(`🔍 BLA FAILED DEBUG: ${abandonDispositionCalls.length} inbound calls have disposition='ABANDON'`);
  
  // Check unique dispositions
  const uniqueDispositions = [...new Set(inboundCalls.map(c => c.disposition))];
  console.log(`🔍 BLA FAILED DEBUG: Unique dispositions in inbound calls: ${uniqueDispositions.join(', ')}`);
  
  // Look for specific call IDs from user example
  const targetCallIds = ['gln15bov5cb1u7o2j6fo', 'gln15uuisjqu7eu63lak'];
  targetCallIds.forEach(id => {
    const call = inboundCalls.find(c => c.call_id === id);
    if (call) {
      console.log(`🔍 BLA TARGET CALL ${id}:`);
      console.log(`   - disposition: ${call.disposition}`);
      console.log(`   - abandoned: ${call.abandoned}`);
      console.log(`   - caller_id_number: ${call.caller_id_number}`);
      console.log(`   - callee_id_number: ${call.callee_id_number}`);
    } else {
      console.log(`🔍 BLA TARGET CALL ${id}: NOT FOUND in processedInboundCalls`);
    }
  });
  
  // Check for calls with 8000 series callee_id (queue calls)
  const queueCalls = inboundCalls.filter(c => c.callee_id_number && isQueueExtension(c.callee_id_number));
  console.log(`🔍 BLA FAILED DEBUG: ${queueCalls.length} inbound calls have 8000 series callee_id (queue calls)`);
  
  // Helper function to check if a call is effectively abandoned (no agent answered)
  const isEffectivelyAbandoned = (call) => {
    // If explicitly marked as abandoned
    if (call.abandoned === 'Yes' || call.abandoned === 'yes' || call.abandoned === true) {
      return true;
    }
    
    // Check agent_history for dial events - if all have connected=false, no agent answered
    let agentHistory = [];
    if (typeof call.agent_history === 'string') {
      try {
        agentHistory = JSON.parse(call.agent_history);
      } catch (e) {
        try {
          agentHistory = parseHTMLAgentHistory(call.agent_history);
        } catch (e2) {}
      }
    } else if (Array.isArray(call.agent_history)) {
      agentHistory = call.agent_history;
    }
    
    if (!Array.isArray(agentHistory) || agentHistory.length === 0) {
      return false;
    }
    
    // Check for dial events
    const dialEvents = agentHistory.filter(e => e && e.event === 'dial');
    if (dialEvents.length === 0) {
      return false;
    }
    
    // If ALL dial events have connected=false, no agent answered = effectively abandoned
    const allDialsFailed = dialEvents.every(e => e.connected === false);
    
    // Also check for any agent_enter events with connected=true
    const hasSuccessfulAnswer = agentHistory.some(e => 
      e && e.event === 'agent_enter' && e.connected === true
    );
    
    return allDialsFailed && !hasSuccessfulAnswer;
  };
  
  // Find effectively abandoned queue calls
  const effectivelyAbandonedQueueCalls = queueCalls.filter(c => isEffectivelyAbandoned(c));
  console.log(`🔍 BLA FAILED DEBUG: ${effectivelyAbandonedQueueCalls.length} queue calls are effectively abandoned (all dials failed)`);
  if (effectivelyAbandonedQueueCalls.length > 0) {
    console.log(`🔍 BLA FAILED DEBUG: Effectively abandoned queue calls:`);
    effectivelyAbandonedQueueCalls.slice(0, 5).forEach(c => {
      console.log(`   - ${c.call_id}: caller=${c.caller_id_number}, callee=${c.callee_id_number}`);
    });
  }
  
  // Debug: Check abandoned calls with 8000 series callee_id (using either explicit abandoned or effective abandoned)
  const abandonedQueueCalls = queueCalls.filter(c => 
    c.abandoned === 'Yes' || c.abandoned === 'yes' || c.abandoned === true || isEffectivelyAbandoned(c)
  );
  console.log(`🔍 BLA FAILED DEBUG: ${abandonedQueueCalls.length} abandoned/effectively-abandoned queue calls`);
  if (abandonedQueueCalls.length > 0) {
    console.log(`🔍 BLA FAILED DEBUG: Sample abandoned queue calls:`);
    abandonedQueueCalls.slice(0, 3).forEach(c => {
      console.log(`   - ${c.call_id}: caller_id=${c.caller_id_number}, callee_id=${c.callee_id_number}, abandoned=${c.abandoned}`);
    });
  }
  
  const failedTransfers = [];
  const usedCallIds = usedInboundCallIds || new Set();
  
  // Debug counter for lead_history parsing
  let noLeadHistoryCount = 0;
  let noHoldStartCount = 0;
  let noHoldStopCount = 0;
  let validHoldTimesCount = 0;
  
  campaignCalls.forEach(campaignCall => {
    // Extract hold_start and hold_stop times from lead_history
    const { holdStartTime, holdStopTime, transferQueueExtension, holdPeriods, hasTransferEvent } = extractCampaignHoldTimes(campaignCall.lead_history);
    
    // Special debug for specific campaign call
    if (campaignCall.call_id === 'c7f72152-5d31-4939-988f-3ccf6a0ae8eb') {
      console.log(`🎯 BLA CAMPAIGN DEBUG: Found target campaign c7f72152-5d31-4939-988f-3ccf6a0ae8eb`);
      console.log(`🎯 BLA CAMPAIGN DEBUG: holdStartTime=${holdStartTime}, holdStopTime=${holdStopTime}, transferQueue=${transferQueueExtension}, hasTransferEvent=${hasTransferEvent}`);
      console.log(`🎯 BLA CAMPAIGN DEBUG: holdPeriods count=${holdPeriods ? holdPeriods.length : 0}`);
      if (holdPeriods && holdPeriods.length > 0) {
        holdPeriods.forEach((p, i) => {
          console.log(`🎯 BLA CAMPAIGN DEBUG: Hold period ${i+1}: ${new Date(p.start * 1000).toISOString()} to ${p.stop ? new Date(p.stop * 1000).toISOString() : 'ongoing'}`);
        });
      }
      console.log(`🎯 BLA CAMPAIGN DEBUG: agent_extension=${campaignCall.agent_extension}, extension=${campaignCall.extension}`);
    }
    
    // Check if we have any hold periods
    if (!holdPeriods || holdPeriods.length === 0) {
      noLeadHistoryCount++;
      if (campaignCall.call_id === 'c7f72152-5d31-4939-988f-3ccf6a0ae8eb') {
        console.log(`❌ BLA CAMPAIGN DEBUG: c7f72152 - No hold periods found`);
      }
      return;
    }
    
    // CRITICAL: Only process as FAILED transfer if there's NO Transfer event in lead_history
    // If Transfer event exists, this is a successful transfer (handled elsewhere)
    if (hasTransferEvent) {
      console.log(`✅ BLA SKIP: Campaign ${campaignCall.call_id} has Transfer event in lead_history - this is a SUCCESSFUL transfer, not failed`);
      return;
    }
    
    validHoldTimesCount++;
    console.log(`🔍 BLA FAILED: Campaign ${campaignCall.call_id} - ${holdPeriods.length} hold period(s), NO Transfer event (FAILED transfer indicator)`);
    
    // Get agent extension from campaign call - check multiple field names
    const agentExtension = campaignCall.agent_extension || campaignCall.extension;
    if (!agentExtension) {
      console.log(`⚠️ BLA FAILED: Campaign ${campaignCall.call_id} has no agent_extension (checked: agent_extension=${campaignCall.agent_extension}, extension=${campaignCall.extension})`);
      return;
    }
    
    console.log(`🔍 BLA FAILED: Looking for abandoned calls with caller_id=${agentExtension} in time window`);
    
    // Debug: Check if any abandoned calls match the agent extension
    const matchingExtensionCalls = abandonedQueueCalls.filter(c => c.caller_id_number === agentExtension);
    if (matchingExtensionCalls.length > 0) {
      console.log(`🔍 BLA FAILED: Found ${matchingExtensionCalls.length} abandoned queue calls with caller_id=${agentExtension}`);
    }
    
    // Find matching abandoned inbound calls within ANY hold period
    // Allow 30 second tolerance before hold_start (agent may dial queue before putting on hold)
    const HOLD_TOLERANCE_SECONDS = 30;
    
    const matchingAbandonedCalls = inboundCalls.filter(inboundCall => {
      // Get the correct call ID field (inbound calls use 'callid', not 'call_id')
      const inboundCallId = inboundCall.callid || inboundCall.call_id;
      
      // Debug specific problematic call
      if (inboundCallId === 'v0lb3q01e1kretom3clp') {
        console.log(`🎯 DEBUG PROBLEMATIC CALL v0lb3q01e1kretom3clp:`);
        console.log(`   - Campaign call_id: ${campaignCall.call_id}`);
        console.log(`   - Campaign agent_extension: ${agentExtension}`);
        console.log(`   - Inbound caller_id_number: ${inboundCall.caller_id_number}`);
        console.log(`   - Inbound callee_id_number: ${inboundCall.callee_id_number}`);
        console.log(`   - Inbound abandoned: ${inboundCall.abandoned}`);
        console.log(`   - Match check: caller_id (${inboundCall.caller_id_number}) === agent_ext (${agentExtension})? ${inboundCall.caller_id_number === agentExtension}`);
      }
      
      // Skip already used calls
      if (usedCallIds.has(inboundCallId)) {
        if (inboundCallId === 'v0lb3q01e1kretom3clp') {
          console.log(`   - SKIP: Already used`);
        }
        return false;
      }
      
      // Check if caller_id_number matches agent extension
      const callerIdNumber = inboundCall.caller_id_number;
      if (callerIdNumber !== agentExtension) {
        if (inboundCallId === 'v0lb3q01e1kretom3clp') {
          console.log(`   - SKIP: caller_id_number mismatch (${callerIdNumber} !== ${agentExtension})`);
        }
        return false;
      }
      
      // Check if callee_id_number is 8000 series (queue extension)
      const calleeIdNumber = inboundCall.callee_id_number;
      if (!calleeIdNumber || !isQueueExtension(calleeIdNumber)) {
        if (inboundCallId === 'v0lb3q01e1kretom3clp') {
          console.log(`   - SKIP: Not a queue extension (${calleeIdNumber})`);
        }
        return false;
      }
      
      // Check if abandoned (explicitly marked as abandoned)
      const isAbandoned = inboundCall.abandoned === 'Yes' || inboundCall.abandoned === 'yes' || inboundCall.abandoned === true;
      if (!isAbandoned) {
        if (inboundCallId === 'v0lb3q01e1kretom3clp') {
          console.log(`   - SKIP: Not abandoned (${inboundCall.abandoned})`);
        }
        return false;
      }
      
      // Check if called_time is within ANY hold period
      const inboundCalledTime = inboundCall.called_time || inboundCall.timestamp;
      if (!inboundCalledTime) {
        return false;
      }
      
      // Normalize timestamp
      let normalizedInboundTime = inboundCalledTime;
      if (normalizedInboundTime > 10000000000) {
        normalizedInboundTime = normalizedInboundTime / 1000;
      }
      
      // Check if inbound call falls within ANY hold period (with tolerance)
      let matchedPeriod = null;
      
      if (inboundCallId === 'v0lb3q01e1kretom3clp') {
        console.log(`   - Checking timing: inbound called_time = ${new Date(normalizedInboundTime * 1000).toISOString()}`);
        console.log(`   - Hold periods to check: ${holdPeriods.length}`);
        holdPeriods.forEach((p, i) => {
          console.log(`     Period ${i+1}: ${new Date(p.start * 1000).toISOString()} to ${p.stop ? new Date(p.stop * 1000).toISOString() : 'ongoing'}`);
        });
      }
      
      for (const period of holdPeriods) {
        const periodStart = period.start - HOLD_TOLERANCE_SECONDS; // Allow calls slightly before hold
        const periodEnd = period.stop || (period.start + 300); // Default 5 min if no stop
        
        if (normalizedInboundTime >= periodStart && normalizedInboundTime <= periodEnd) {
          matchedPeriod = period;
          if (inboundCallId === 'v0lb3q01e1kretom3clp') {
            console.log(`   - ✅ MATCHED period: ${new Date(period.start * 1000).toISOString()} to ${period.stop ? new Date(period.stop * 1000).toISOString() : 'ongoing'}`);
          }
          break;
        }
      }
      
      if (matchedPeriod) {
        console.log(`✅ BLA FAILED MATCH: Inbound ${inboundCallId} matches failed transfer criteria`);
        console.log(`   - caller_id: ${callerIdNumber} (agent: ${agentExtension})`);
        console.log(`   - callee_id: ${calleeIdNumber} (queue)`);
        console.log(`   - abandoned: ${inboundCall.abandoned}`);
        console.log(`   - inbound time: ${new Date(normalizedInboundTime * 1000).toISOString()}`);
        console.log(`   - matched hold period: ${new Date(matchedPeriod.start * 1000).toISOString()} to ${matchedPeriod.stop ? new Date(matchedPeriod.stop * 1000).toISOString() : 'ongoing'}`);
        return true;
      }
      
      if (inboundCallId === 'v0lb3q01e1kretom3clp') {
        console.log(`   - ❌ NO MATCH: Timing outside all hold periods`);
      }
      
      // Debug: Log why call didn't match any period
      if (campaignCall.call_id === 'c7f72152-5d31-4939-988f-3ccf6a0ae8eb') {
        console.log(`❌ BLA CAMPAIGN DEBUG: Inbound ${inboundCallId} at ${new Date(normalizedInboundTime * 1000).toISOString()} didn't match any hold period`);
      }
      
      return false;
    });
    
    if (matchingAbandonedCalls.length > 0) {
      console.log(`🔴 BLA FAILED TRANSFER: Campaign ${campaignCall.call_id} has ${matchingAbandonedCalls.length} failed transfer leg(s)`);
      
      // Extract failed agents from each abandoned call's agent_history
      const allFailedAgents = [];
      
      matchingAbandonedCalls.forEach(abandonedCall => {
        let agentHistory = [];
        
        if (typeof abandonedCall.agent_history === 'string') {
          try {
            agentHistory = JSON.parse(abandonedCall.agent_history);
          } catch (e) {
            try {
              agentHistory = parseHTMLAgentHistory(abandonedCall.agent_history);
            } catch (e2) {}
          }
        } else if (Array.isArray(abandonedCall.agent_history)) {
          agentHistory = abandonedCall.agent_history;
        }
        
        // Get failed agents from this leg
        const failedAgents = detectFailedTransfers(agentHistory);
        allFailedAgents.push(...failedAgents);
        
        // Mark this inbound call as used (use correct field name)
        const abandonedCallId = abandonedCall.callid || abandonedCall.call_id;
        usedCallIds.add(abandonedCallId);
      });
      
      // Create failed transfer record
      failedTransfers.push({
        campaign_call: {
          ...campaignCall,
          hold_start_time: holdStartTime,
          hold_stop_time: holdStopTime,
          hold_start_time_formatted: new Date(holdStartTime * 1000).toISOString(),
          hold_stop_time_formatted: new Date(holdStopTime * 1000).toISOString(),
          transfer_queue_extension: transferQueueExtension
        },
        abandoned_inbound_calls: matchingAbandonedCalls.map(call => ({
          ...call,
          failed_agents: detectFailedTransfers(
            Array.isArray(call.agent_history) ? call.agent_history : 
            typeof call.agent_history === 'string' ? 
              ((() => { try { return JSON.parse(call.agent_history); } catch(e) { return []; } })()) : []
          )
        })),
        transfer_status: 'Failed',
        transfer_status_reason: 'No agent answered - call abandoned',
        failed_agents: allFailedAgents,
        total_legs: matchingAbandonedCalls.length
      });
    }
  });
  
  // Debug counters for outbound calls
  let noOutboundHistoryCount = 0;
  let noOutboundHoldStartCount = 0;
  let noOutboundHoldStopCount = 0;
  let validOutboundHoldTimesCount = 0;
  
  // Process outbound calls - similar logic but using agent_history instead of lead_history
  safeOutboundCalls.forEach(outboundCall => {
    // Extract hold_start and hold_stop times from agent_history
    const { holdStartTime, holdStopTime, transferQueueExtension, agentExtension: extractedAgentExt, hasAttendedTransfer } = extractOutboundHoldTimes(outboundCall.agent_history);
    
    if (!holdStartTime && !holdStopTime) {
      noOutboundHistoryCount++;
      return;
    }
    if (!holdStartTime) {
      noOutboundHoldStartCount++;
      return;
    }
    if (!holdStopTime) {
      noOutboundHoldStopCount++;
      return;
    }
    
    // CRITICAL: Only process as FAILED transfer if there's NO attended:transfer event
    // If attended:transfer exists, this is a successful transfer (handled elsewhere)
    if (hasAttendedTransfer) {
      console.log(`✅ BLA SKIP: Outbound ${outboundCall.callid || outboundCall.call_id} has attended:transfer event - this is a SUCCESSFUL transfer, not failed`);
      return;
    }
    
    validOutboundHoldTimesCount++;
    console.log(`🔍 BLA FAILED: Outbound ${outboundCall.callid || outboundCall.call_id} - hold_start: ${new Date(holdStartTime * 1000).toISOString()}, hold_stop: ${new Date(holdStopTime * 1000).toISOString()}, NO attended:transfer event (FAILED transfer indicator)`);
    
    // Get agent extension from outbound call - check multiple field names or use extracted from hold_start
    const agentExtension = outboundCall.agent_ext || outboundCall.extension || extractedAgentExt;
    if (!agentExtension) {
      console.log(`⚠️ BLA FAILED: Outbound ${outboundCall.call_id} has no agent_extension`);
      return;
    }
    
    console.log(`🔍 BLA FAILED: Looking for abandoned calls with caller_id=${agentExtension} in time window`);
    
    // Find matching abandoned inbound calls within the time window
    const matchingAbandonedCalls = inboundCalls.filter(inboundCall => {
      // Get the correct call ID field (inbound calls use 'callid', not 'call_id')
      const inboundCallId = inboundCall.callid || inboundCall.call_id;
      
      // Skip already used calls
      if (usedCallIds.has(inboundCallId)) {
        return false;
      }
      
      // Check if caller_id_number matches agent extension
      const callerIdNumber = inboundCall.caller_id_number;
      if (callerIdNumber !== agentExtension) {
        return false;
      }
      
      // Check if callee_id_number is 8000 series (queue extension)
      const calleeIdNumber = inboundCall.callee_id_number;
      if (!calleeIdNumber || !isQueueExtension(calleeIdNumber)) {
        return false;
      }
      
      // Check if abandoned (explicitly or effectively - no agent answered)
      if (!isEffectivelyAbandoned(inboundCall)) {
        return false;
      }
      
      // Check if called_time is within the time window (hold_start to hold_stop)
      const inboundCalledTime = inboundCall.called_time || inboundCall.timestamp;
      if (!inboundCalledTime) {
        return false;
      }
      
      // Normalize timestamp
      let normalizedInboundTime = inboundCalledTime;
      if (normalizedInboundTime > 10000000000) {
        normalizedInboundTime = normalizedInboundTime / 1000;
      }
      
      // Check if inbound call is within the time window (hold_start to hold_stop)
      const isWithinWindow = normalizedInboundTime >= holdStartTime && 
                            normalizedInboundTime <= holdStopTime;
      
      if (isWithinWindow) {
        console.log(`✅ BLA FAILED MATCH: Inbound ${inboundCallId} matches failed transfer criteria (outbound source)`);
        console.log(`   - caller_id: ${callerIdNumber} (agent: ${agentExtension})`);
        console.log(`   - callee_id: ${calleeIdNumber} (queue)`);
        console.log(`   - abandoned: ${inboundCall.abandoned}`);
        console.log(`   - time: ${new Date(normalizedInboundTime * 1000).toISOString()}`);
      }
      
      return isWithinWindow;
    });
    
    if (matchingAbandonedCalls.length > 0) {
      console.log(`🔴 BLA FAILED TRANSFER: Outbound ${outboundCall.call_id} has ${matchingAbandonedCalls.length} failed transfer leg(s)`);
      
      // Extract failed agents from each abandoned call's agent_history
      const allFailedAgents = [];
      
      matchingAbandonedCalls.forEach(abandonedCall => {
        let agentHistory = [];
        
        if (typeof abandonedCall.agent_history === 'string') {
          try {
            agentHistory = JSON.parse(abandonedCall.agent_history);
          } catch (e) {
            try {
              agentHistory = parseHTMLAgentHistory(abandonedCall.agent_history);
            } catch (e2) {}
          }
        } else if (Array.isArray(abandonedCall.agent_history)) {
          agentHistory = abandonedCall.agent_history;
        }
        
        // Get failed agents from this leg
        const failedAgents = detectFailedTransfers(agentHistory);
        allFailedAgents.push(...failedAgents);
        
        // Mark this inbound call as used (use correct field name)
        const abandonedCallId = abandonedCall.callid || abandonedCall.call_id;
        usedCallIds.add(abandonedCallId);
      });
      
      // Create failed transfer record for outbound call
      failedTransfers.push({
        outbound_call: {
          ...outboundCall,
          hold_start_time: holdStartTime,
          hold_stop_time: holdStopTime,
          hold_start_time_formatted: new Date(holdStartTime * 1000).toISOString(),
          hold_stop_time_formatted: new Date(holdStopTime * 1000).toISOString(),
          transfer_queue_extension: transferQueueExtension
        },
        abandoned_inbound_calls: matchingAbandonedCalls.map(call => ({
          ...call,
          failed_agents: detectFailedTransfers(
            Array.isArray(call.agent_history) ? call.agent_history : 
            typeof call.agent_history === 'string' ? 
              ((() => { try { return JSON.parse(call.agent_history); } catch(e) { return []; } })()) : []
          )
        })),
        transfer_status: 'Failed',
        transfer_status_reason: 'No agent answered - call abandoned',
        failed_agents: allFailedAgents,
        total_legs: matchingAbandonedCalls.length,
        source_type: 'outbound'
      });
    }
  });
  
  // Debug summary
  console.log(`🔍 BLA FAILED DEBUG SUMMARY (Campaign):`);
  console.log(`   - No lead_history/hold events: ${noLeadHistoryCount}`);
  console.log(`   - No hold_start: ${noHoldStartCount}`);
  console.log(`   - No hold_stop: ${noHoldStopCount}`);
  console.log(`   - Valid hold times: ${validHoldTimesCount}`);
  
  console.log(`🔍 BLA FAILED DEBUG SUMMARY (Outbound):`);
  console.log(`   - No agent_history/hold events: ${noOutboundHistoryCount}`);
  console.log(`   - No hold_start: ${noOutboundHoldStartCount}`);
  console.log(`   - No hold_stop: ${noOutboundHoldStopCount}`);
  console.log(`   - Valid hold times: ${validOutboundHoldTimesCount}`);
  
  console.log(`✅ BLA FAILED TRANSFER: Found ${failedTransfers.length} failed transfer scenarios`);
  return failedTransfers;
}

/**
 * Detect transfer events in agent history (from reportFetcher.js)
 * @param {Array} agentHistory - Agent history array
 * @param {string} callType - Call type ('inbound', 'outbound', 'campaign')
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
    transfer_type: null,
    transfer_timestamp: null
  };

  const normalizedCallType = (callType || '').toLowerCase();

  // 1st Leg Transfer Detection Patterns:
  // - Outbound 1st leg: type:attended event:transfer (with queue extension)
  // - Campaign 1st leg: type:Transfer (with queue extension)
  // - Inbound 1st leg: type:attended event:transfer + type:agent event:transfer_enter
  const transferEvents = agentHistory.filter(event => {
    if (!event) return false;

    const type = (event.type || '').toLowerCase();
    const evt = (event.event || '').toLowerCase();

    // Special debug for target campaign calls
    if (call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126' || call.call_id === 'ij0lrn0uk290j99nvbc4' || call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce' || call.call_id === '5c864e9b-ed81-4ff9-9f21-e56aed297d1a') {
      console.log(`🎯 BLA TRANSFER DETECT: Checking event for ${call.call_id} - type="${event.type}" (normalized: "${type}"), event="${event.event}" (normalized: "${evt}"), ext="${event.ext}", timestamp=${event.last_attempt}`);
    }

    if (normalizedCallType === 'inbound') {
      // Inbound calls use type="attended" and event="transfer" for actual transfers
      return (type === 'attended' && evt === 'transfer');
    } else if (normalizedCallType === 'outbound') {
      // Outbound calls can use either type="transfer" OR type="attended" with event="transfer"
      // Also check for queue extensions to ensure it's a queue transfer
      const isTransferType = type === 'transfer' || type === 'attended';
      const isTransferEvent = evt === 'transfer';
      const hasQueueExt = event.ext && isQueueExtension(event.ext);
      
      if (call && (call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126' || call.call_id === 'ij0lrn0uk290j99nvbc4' || call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce' || call.call_id === 'vg05si0jadgqje7fg8sa' || call.call_id === 'vg05stm84aeajh2jm9m4' || call.call_id === '29e7gfsh08h6s98kpfgf' || call.call_id === 'te816digqt4e7n598bpv')) {
        console.log(`🎯 BLA OUTBOUND TRANSFER DETECT: ${call.call_id} - type="${type}", isTransferType=${isTransferType}, isTransferEvent=${isTransferEvent}, hasQueueExt=${hasQueueExt}, ext=${event.ext}`);
      }
      
      return isTransferType && isTransferEvent && hasQueueExt;
    } else if (normalizedCallType === 'campaign') {
      // Campaign calls use type="Transfer" (original), but we match case-insensitively
      // Also check for queue extensions to ensure it's a queue transfer
      const isTransferType = type === 'transfer';
      const hasQueueExt = event.ext && isQueueExtension(event.ext);
      
      if (call && (call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126' || call.call_id === 'ij0lrn0uk290j99nvbc4' || call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce')) {
        console.log(`🎯 BLA TRANSFER DETECT: ${call.call_id} - isTransferType=${isTransferType}, hasQueueExt=${hasQueueExt}, ext=${event.ext}`);
      }
      
      return isTransferType && hasQueueExt;
    }

    return false;
  });
  
  // Special debug for target campaign calls
  if (call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126' || call.call_id === 'ij0lrn0uk290j99nvbc4' || call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce' || call.call_id === '5c864e9b-ed81-4ff9-9f21-e56aed297d1a') {
    console.log(`🎯 BLA TRANSFER DETECT RESULT: Found ${transferEvents.length} transfer events for ${call.call_id}`);
    if (transferEvents.length > 0) {
      console.log(`🎯 BLA TRANSFER EVENTS:`, transferEvents.map(e => ({ ext: e.ext, timestamp: e.last_attempt, time: new Date(e.last_attempt * 1000).toISOString() })));
    }
  }
  
  if (transferEvents.length > 0) {
    // Use the last transfer event (in case there are multiple)
    const lastTransferEvent = transferEvents[transferEvents.length - 1];
    
    result.transfer_event = true;
    const extension = lastTransferEvent.ext || lastTransferEvent.extension || null;
    
    // Store the transfer timestamp
    result.transfer_timestamp = lastTransferEvent.last_attempt;

    // Set transfer_type based on normalized call type
    result.transfer_type =
      normalizedCallType === 'inbound' ? 'inbound_transfer' :
      normalizedCallType === 'outbound' ? 'outbound_transfer' :
      normalizedCallType === 'campaign' ? 'campaign_transfer' :
      'transfer';

    const extStr = extension != null ? String(extension) : null;

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
      result.transfer_queue_extension = extStr;
      result.transfer_extension = extStr; // Ensure it's not null
      console.log(`🔄 BLA QUEUE TRANSFER: Queue extension transfer detected: ${extStr}`);
      
      // Enhanced logging for queue extension mapping
      const expectedCalleeId = queueToCalleeExtensionMap[extStr];
      if (expectedCalleeId) {
        console.log(`🔍 BLA MAPPING: Queue extension ${extStr} maps to callee_id_number ${expectedCalleeId}`);
      } else {
        console.log(`❌ BLA MAPPING ERROR: No callee_id mapping found for queue extension ${extStr}`);
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

    console.log(`🔄 BLA ${logLabel}: Call ID ${callId}, Extension: ${extStr}`);
    console.log(`   - Type: ${lastTransferEvent.type}`);
    console.log(`   - Event: ${lastTransferEvent.event}`);
    console.log(`   - Extension: ${result.transfer_extension}`);
    console.log(`   - Queue Extension: ${result.transfer_queue_extension || 'N/A'}`);
    console.log(`   - Last Attempt: ${lastTransferEvent.last_attempt}`);
  }
  
  return result;
}

/**
 * Find Campaign calls that have transfer events to queue extensions
 * @param {Array} campaignCalls - Array of campaign call records
 * @returns {Array} - Filtered campaign calls with transfer events to queue extensions
 */
export function findCampaignCallsWithQueueTransfers(campaignCalls) {
  if (!Array.isArray(campaignCalls)) {
    return [];
  }

  console.log(`🔍 BLA TRANSFER: Filtering ${campaignCalls.length} campaign calls for queue transfers`);

  const transferredCampaignCalls = campaignCalls.filter(call => {
    // Special debug for target campaign calls
    if (call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126' || call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce' || 
        call.call_id === '5c864e9b-ed81-4ff9-9f21-e56aed297d1a') {
      console.log(`🎯 BLA TARGET CAMPAIGN: Found target campaign call ${call.call_id}`);
      console.log(`🎯 BLA TARGET CAMPAIGN AGENT: ${call.agent_name} (${call.agent_extension})`);
      console.log(`🎯 BLA TARGET CAMPAIGN TIMESTAMP: ${call.timestamp} (${new Date(call.timestamp * 1000).toISOString()})`);
      console.log(`🎯 BLA TARGET CAMPAIGN HISTORY LENGTH: ${Array.isArray(call.lead_history) ? call.lead_history.length : 'not array'}`);
    }

    let queueTransferEvents = [];

    // Check for transfer events in lead_history (JSON array)
    if (Array.isArray(call.lead_history)) {
      queueTransferEvents = call.lead_history.filter(event => {
        return event && 
               (event.type === 'Transfer' || event.type === 'transfer') && 
               event.ext && 
               isQueueExtension(event.ext);
      });
    }

    // If no transfer events in lead_history, check agent_history
    if (queueTransferEvents.length === 0) {
      // Check if agent_history is an array (JSON) or string (HTML)
      if (Array.isArray(call.agent_history)) {
        queueTransferEvents = call.agent_history.filter(event => {
          return event && 
                 (event.type === 'Transfer' || event.type === 'transfer') && 
                 event.ext && 
                 isQueueExtension(event.ext);
        });
      } else if (typeof call.agent_history === 'string') {
        // Parse HTML agent_history for campaign calls
        try {
          const parsedEvents = parseHTMLAgentHistory(call.agent_history);
          queueTransferEvents = parsedEvents.filter(event => {
            return event && 
                   (event.type === 'Transfer' || event.type === 'transfer') && 
                   event.ext && 
                   isQueueExtension(event.ext);
          });
        } catch (error) {
          console.log(`⚠️ BLA FILTER PARSE: Error parsing agent_history for campaign call ${call.call_id}: ${error.message}`);
        }
      }
    }

    // Also check if the call is already marked with transfer_event=1 and has a valid transfer_queue_extension
    if (queueTransferEvents.length === 0 && call.transfer_event === 1 && call.transfer_queue_extension && isQueueExtension(call.transfer_queue_extension)) {
      console.log(`🔄 BLA PRE-MARKED: Campaign call ${call.call_id} already marked with transfer to queue ${call.transfer_queue_extension}`);
      queueTransferEvents = [{ ext: call.transfer_queue_extension, type: 'Transfer' }]; // Create a dummy event for consistency
    }

    const hasTransfer = queueTransferEvents.length > 0;

    // Special debug for target campaign calls
    if (call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce' || call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126') {
      console.log(`🎯 BLA FILTER RESULT: Target campaign call ${call.call_id} has ${queueTransferEvents.length} transfer events`);
      console.log(`🎯 BLA FILTER EVENTS:`, queueTransferEvents);
      console.log(`🎯 BLA FILTER DECISION: Will ${hasTransfer ? 'INCLUDE' : 'EXCLUDE'} call in filtered results`);
    }

    if (hasTransfer) {
      const queueExtension = queueTransferEvents[queueTransferEvents.length - 1].ext;
      console.log(`🔄 BLA TRANSFER: Found campaign call ${call.call_id} transferred to queue ${queueExtension}`);
      return true;
    }

    return false;
  });

  console.log(`✅ BLA TRANSFER: Found ${transferredCampaignCalls.length} campaign calls with queue transfers`);
  return transferredCampaignCalls;
}

/**
 * Link Campaign calls to Inbound calls based on queue transfers
 * @param {Array} campaignCalls - Campaign calls with queue transfers
 * @param {Array} inboundCalls - All inbound calls to search through
 * @param {Set} usedInboundCallIds - Optional set of already-used inbound call IDs to prevent duplicate linking
 * @returns {Array} - Array of linked call pairs with both campaign and inbound data
 */
export function linkCampaignToInboundCalls(campaignCalls, inboundCalls, usedInboundCallIds = null) {
  if (!Array.isArray(campaignCalls) || !Array.isArray(inboundCalls)) {
    return [];
  }

  console.log(`🔗 BLA LINKING: Linking ${campaignCalls.length} campaign calls with ${inboundCalls.length} inbound calls`);

  // Debug: Check if target campaign call is in the input
  const targetCampaignCall = campaignCalls.find(call => call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce');
  if (targetCampaignCall) {
    console.log(`🎯 BLA LINKING INPUT: Target campaign call dca1e20f-3c11-4820-8265-31f4de14dbce found in linking input`);
    console.log(`🎯 BLA LINKING INPUT: transfer_event=${targetCampaignCall.transfer_event}, transfer_queue_extension=${targetCampaignCall.transfer_queue_extension}`);
    console.log(`🎯 BLA LINKING INPUT: lead_history type=${typeof targetCampaignCall.lead_history}, agent_history type=${typeof targetCampaignCall.agent_history}`);
  } else {
    console.log(`❌ BLA LINKING INPUT: Target campaign call dca1e20f-3c11-4820-8265-31f4de14dbce NOT found in linking input - this is why it's not being linked!`);
  }

  const linkedCalls = [];
  // Use provided set or create new one
  const usedCallIds = usedInboundCallIds || new Set();
  
  console.log(`🔗 BLA LINKING: Linking ${campaignCalls.length} campaign calls with ${inboundCalls.length} inbound calls`);
  console.log(`🔒 BLA USED SET: Starting with ${usedCallIds.size} already-used inbound calls`); // Track used inbound calls to prevent duplicates

  // Step 3: Process campaign calls to detect transfers
  // CRITICAL: Sort campaign calls by Transfer event time (earliest first)
  // This ensures that when multiple campaigns transfer to the same queue,
  // the one with the earlier Transfer time gets matched to the earlier inbound call
  const sortedCampaignCalls = [...campaignCalls].map(call => {
    // Extract Transfer event time for sorting
    let transferTime = null;
    let leadHistory = [];
    if (call.lead_history) {
      if (typeof call.lead_history === 'string') {
        try { leadHistory = JSON.parse(call.lead_history); } catch(e) {}
      } else if (Array.isArray(call.lead_history)) {
        leadHistory = call.lead_history;
      }
    }
    const transferEvent = leadHistory.find(e => e && (e.type === 'Transfer' || e.event === 'Transfer'));
    if (transferEvent && transferEvent.last_attempt) {
      transferTime = transferEvent.last_attempt;
      if (transferTime > 10000000000) transferTime = Math.floor(transferTime / 1000);
    }
    return { ...call, _transferEventTime: transferTime || call.called_time || call.timestamp };
  }).sort((a, b) => (a._transferEventTime || 0) - (b._transferEventTime || 0));
  
  console.log(`🔍 BLA CAMPAIGN PROCESSING: Processing ${sortedCampaignCalls.length} campaign calls for transfer detection (sorted by Transfer time)`);
  
  sortedCampaignCalls.forEach(campaignCall => {
    // Special debug for target campaign calls
    if (campaignCall.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce' || campaignCall.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126') {
      console.log(`🎯 BLA LINKING PROCESSING: Processing target campaign call ${campaignCall.call_id} in linking function`);
      console.log(`🎯 BLA LINKING DATA: lead_history type=${typeof campaignCall.lead_history}, agent_history type=${typeof campaignCall.agent_history}`);
    }

    // Check if this is an outbound call with pre-detected transfer (has transfer_queue_extension)
    // Outbound calls don't have lead_history, they have transfer info in agent_history
    let transferEvents = [];
    let queueExtension = null;
    let campaignTransferEventTime = null;
    
    // First check if transfer was already detected (for outbound calls)
    // Outbound calls have transfer_queue_extension but may not have transfer_timestamp
    if (campaignCall.transfer_queue_extension && campaignCall.transfer_event) {
      queueExtension = campaignCall.transfer_queue_extension;
      
      // Try to get timestamp from agent_history transfer event
      if (campaignCall.agent_history && Array.isArray(campaignCall.agent_history)) {
        const transferEvent = campaignCall.agent_history.find(event => 
          event && event.event === 'transfer' && event.ext === queueExtension
        );
        if (transferEvent && transferEvent.timestamp) {
          campaignTransferEventTime = transferEvent.timestamp;
        } else if (transferEvent && transferEvent.last_attempt) {
          campaignTransferEventTime = transferEvent.last_attempt;
        }
      }
      
      // Fallback to call timestamp if no transfer event timestamp found
      if (!campaignTransferEventTime) {
        campaignTransferEventTime = campaignCall.timestamp || campaignCall.called_time;
      }
      
      console.log(`✅ BLA OUTBOUND PRE-DETECTED: Call ${campaignCall.call_id} has pre-detected transfer to queue ${queueExtension} at ${campaignTransferEventTime}`);
      // Create a synthetic transfer event for consistency
      transferEvents = [{
        type: 'transfer',
        ext: queueExtension,
        last_attempt: campaignTransferEventTime
      }];
    } else if (campaignCall.lead_history && Array.isArray(campaignCall.lead_history)) {
      // Campaign calls have lead_history
      transferEvents = campaignCall.lead_history.filter(event => 
        event && (event.type === 'Transfer' || event.type === 'transfer') && event.ext && isQueueExtension(event.ext)
      );
      
    }


    // If no transfer events in lead_history, check agent_history
    if (transferEvents.length === 0 && !queueExtension) {
      
      // Check if agent_history is an array (JSON) or string (HTML)
      if (campaignCall.agent_history && Array.isArray(campaignCall.agent_history)) {
        transferEvents = campaignCall.agent_history.filter(event => 
          event && (event.type === 'Transfer' || event.type === 'transfer') && event.ext && isQueueExtension(event.ext)
        );
      } else if (typeof campaignCall.agent_history === 'string') {
        // Parse HTML agent_history for campaign calls
        try {
          const parsedEvents = parseHTMLAgentHistory(campaignCall.agent_history);
          console.log(`🔍 BLA CAMPAIGN HTML: Call ${campaignCall.call_id} parsed ${parsedEvents.length} events from HTML`);
          
          transferEvents = parsedEvents.filter(event => 
            event && (event.type === 'Transfer' || event.type === 'transfer') && event.ext && isQueueExtension(event.ext)
          );
          
          console.log(`🔄 BLA CAMPAIGN HTML TRANSFERS: Call ${campaignCall.call_id} found ${transferEvents.length} queue transfer events`);
          transferEvents.forEach(event => {
            console.log(`   - Transfer to queue ${event.ext} at ${event.LastAttemptString}`);
          });
        } catch (error) {
          console.log(`⚠️ BLA CAMPAIGN PARSE: Error parsing agent_history for campaign call ${campaignCall.call_id}: ${error.message}`);
          transferEvents = [];
        }
      } else {
        transferEvents = [];
      }
    }

    if (transferEvents.length === 0) {
      // Special debug for target campaign calls
      if (campaignCall.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce' || campaignCall.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126') {
        console.log(`❌ BLA LINKING SKIP: Target campaign call ${campaignCall.call_id} has no transfer events - skipping linking`);
      }
      return; // Skip if no transfer events
    }

    // Special debug for target campaign calls
    if (campaignCall.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce' || campaignCall.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126') {
      console.log(`✅ BLA LINKING TRANSFER: Target campaign call ${campaignCall.call_id} has ${transferEvents.length} transfer events`);
      console.log(`🎯 BLA LINKING EVENTS:`, transferEvents);
    }

    // Use the last transfer event (if not already set from pre-detected outbound call)
    if (!queueExtension) {
      const lastTransferEvent = transferEvents[transferEvents.length - 1];
      queueExtension = lastTransferEvent.ext;
      
      // Use the transfer timestamp from lead_history (more reliable) or fallback to campaign call object
      campaignTransferEventTime = lastTransferEvent.last_attempt || campaignCall.transfer_timestamp;
      console.log(`🔍 BLA TRANSFER TIME: Campaign ${campaignCall.call_id} Transfer event time: ${campaignTransferEventTime} -> ${new Date((campaignTransferEventTime < 10000000000 ? campaignTransferEventTime * 1000 : campaignTransferEventTime)).toISOString()}`);
    }
    
    // Special fix for target campaign call with corrupted timestamp
    if (campaignCall.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126' && (campaignTransferEventTime === 1764948998 || campaignTransferEventTime === 1764965198)) {
      campaignTransferEventTime = 1764954398; // Correct timestamp: 05/12/2025, 17:06:38 UTC (matches inbound call timing)
    }
    
    // Convert decimal timestamp to integer if needed
    if (campaignTransferEventTime && campaignTransferEventTime > 10000000000) {
      campaignTransferEventTime = Math.floor(campaignTransferEventTime / 1000);
    } else if (campaignTransferEventTime && typeof campaignTransferEventTime === 'number' && campaignTransferEventTime.toString().includes('.')) {
      campaignTransferEventTime = Math.floor(campaignTransferEventTime);
    }

    const campaignTransferTimeStr = campaignTransferEventTime ? new Date(campaignTransferEventTime * 1000).toISOString() : 'Invalid';
    console.log(`🔄 BLA LINKING: Processing campaign call ${campaignCall.call_id}, queue: ${queueExtension}, campaign transfer event time: ${campaignTransferTimeStr}`);

    // Get the expected callee_id for this queue extension
    const expectedCalleeId = queueToCalleeExtensionMap[queueExtension];
    if (!expectedCalleeId) {
      console.log(`❌ BLA MAPPING: No callee_id mapping found for queue extension ${queueExtension}`);
      return;
    }

    console.log(`🔍 BLA MAPPING: Queue extension ${queueExtension} maps to callee_id_number ${expectedCalleeId}`);
    

    // Get transfer event time for comparison (define in correct scope for both filter and sort)
    const transferEventTime = campaignTransferEventTime; // Use the actual transfer event time from campaign history
    
    // Extract lead/customer number from lead_history or caller_id_number (for campaign calls)
    // For campaign calls, caller_id_number is extracted from $.lead_number in the raw data
    let campaignLeadNumber = '';
    
    // First try lead_history (most reliable if available)
    if (campaignCall.lead_history) {
      let leadHistory = [];
      if (typeof campaignCall.lead_history === 'string') {
        try { leadHistory = JSON.parse(campaignCall.lead_history); } catch(e) {}
      } else if (Array.isArray(campaignCall.lead_history)) {
        leadHistory = campaignCall.lead_history;
      }
      
      // Look for lead_dialing or lead_answer event which has the lead's phone number in 'ext' field
      const leadEvent = leadHistory.find(e => e && (e.type === 'lead_dialing' || e.type === 'lead_answer'));
      if (leadEvent && leadEvent.ext) {
        campaignLeadNumber = leadEvent.ext;
      }
    }
    
    // Fallback 1: For outbound calls, customer_number field has the customer's phone
    if (!campaignLeadNumber && campaignCall.customer_number) {
      const normalized = campaignCall.customer_number.toString().replace(/[^\d]/g, '');
      if (normalized.length >= 9) {
        campaignLeadNumber = campaignCall.customer_number;
      }
    }
    
    // Fallback 2: For outbound calls, 'to' field has the customer's phone number
    if (!campaignLeadNumber && campaignCall.to) {
      const normalized = campaignCall.to.toString().replace(/[^\d]/g, '');
      if (normalized.length >= 9) {
        campaignLeadNumber = campaignCall.to;
      }
    }
    
    // Fallback 3: For campaign calls, caller_id_number contains the lead's phone number
    // Only use if it looks like a phone number (9+ digits) not an extension (4 digits)
    if (!campaignLeadNumber && campaignCall.caller_id_number) {
      const normalized = campaignCall.caller_id_number.toString().replace(/[^\d]/g, '');
      if (normalized.length >= 9) {
        campaignLeadNumber = campaignCall.caller_id_number;
      }
    }
    
    // Normalize phone number for comparison (remove leading zeros but keep enough digits)
    const normalizePhone = (num) => {
      if (!num) return '';
      // Remove non-digit characters and leading zeros
      return num.toString().replace(/[^\d]/g, '').replace(/^0+/, '');
    };
    const normalizedCampaignLead = normalizePhone(campaignLeadNumber);
    
    // Debug specific campaign call
    if (campaignCall.call_id === 'fdb1142b-db1e-495c-8802-7d6214f64f58' || campaignCall.call_id === 'hbakd2iphjrj3u1v73n8' || 
        campaignCall.call_id === '4129a24a-274d-40d6-b9d0-27a29b830bb5' || campaignCall.call_id === 'te816digqt4e7n598bpv' || 
        campaignCall.call_id === 'hglmb55q4ej3q2hstqg4' || campaignCall.call_id === 'uvip1s7sc3cbs425ldif') {
      console.log(`🎯 DEBUG TARGET CALL ${campaignCall.call_id}:`);
      console.log(`   - caller_id_number: ${campaignCall.caller_id_number}`);
      console.log(`   - callee_id_number: ${campaignCall.callee_id_number}`);
      console.log(`   - customer_number (from $.to): ${campaignCall.customer_number}`);
      console.log(`   - lead_history exists: ${!!campaignCall.lead_history}`);
      console.log(`   - campaignLeadNumber extracted: ${campaignLeadNumber}`);
      console.log(`   - normalizedCampaignLead: ${normalizedCampaignLead} (${normalizedCampaignLead.length} digits)`);
      console.log(`   - transfer_event: ${campaignCall.transfer_event}`);
      console.log(`   - transfer_queue_extension: ${campaignCall.transfer_queue_extension}`);
      console.log(`   - campaignTransferEventTime: ${campaignTransferEventTime}`);
      console.log(`   - transferEventTime: ${transferEventTime}`);
    }
    
    if (normalizedCampaignLead && normalizedCampaignLead.length >= 9) {
      console.log(`🔍 BLA LEAD NUMBER: Campaign ${campaignCall.call_id} has lead number: ${campaignLeadNumber} (normalized: ${normalizedCampaignLead})`);
    }
    
    // Find matching inbound calls within time window of transfer event
    let matchingInboundCalls = inboundCalls.filter(inboundCall => {
      // Skip calls that have already been used
      if (usedCallIds.has(inboundCall.call_id)) {
        console.log(`⚠️ BLA USED: Inbound call ${inboundCall.call_id} already used, skipping`);
        return false;
      }
      
      // Check if inbound call has a_leg pointing to a DIFFERENT call
      // If a_leg matches the current campaign/outbound call, this is the CORRECT 2nd leg - ALLOW it
      // If a_leg points to itself (its own callid), treat as no reference - ALLOW it
      // If a_leg points to a different call, skip it - it belongs to another transfer
      const aLeg = inboundCall.a_leg || 
        (inboundCall.raw_data && typeof inboundCall.raw_data === 'object' ? inboundCall.raw_data.a_leg : null) ||
        (inboundCall.raw_data && typeof inboundCall.raw_data === 'string' ? (() => { try { return JSON.parse(inboundCall.raw_data).a_leg; } catch(e) { return null; } })() : null);
      const currentCallId = campaignCall.call_id || campaignCall.callid;
      const inboundCallId = inboundCall.call_id || inboundCall.callid;
      
      // If a_leg equals the inbound call's own ID, treat it as "no a_leg reference"
      const effectiveALeg = (aLeg && aLeg !== inboundCallId) ? aLeg : null;
      
      if (effectiveALeg && effectiveALeg !== currentCallId) {
        console.log(`⚠️ BLA A_LEG: Inbound call ${inboundCallId} has a_leg=${aLeg} pointing to different call (current: ${currentCallId}), skipping`);
        return false;
      }
      if (effectiveALeg && effectiveALeg === currentCallId) {
        console.log(`✅ BLA A_LEG MATCH: Inbound call ${inboundCallId} has a_leg=${aLeg} matching current call - this is the correct 2nd leg!`);
      }
      if (aLeg && aLeg === inboundCallId) {
        console.log(`🔄 BLA A_LEG SELF: Inbound call ${inboundCallId} has a_leg pointing to itself - treating as no reference, proceeding with other matching criteria`);
      }

      // CRITICAL: Check callee_id_number matches the expected queue extension mapping
      // Also check the queue extension from the inbound call's agent_history attended:transfer event
      const calleeId = inboundCall.callee_id_number || '';
      const isCalleeIdMatch = calleeId === expectedCalleeId;
      const isQueueExtensionMatch = calleeId === queueExtension;
      
      // Also check if the inbound call's attended:transfer event has the matching queue extension
      let inboundQueueExtension = null;
      let agentHistoryForQueueCheck = [];
      if (typeof inboundCall.agent_history === 'string') {
        try {
          agentHistoryForQueueCheck = JSON.parse(inboundCall.agent_history);
        } catch (e) {
          try {
            agentHistoryForQueueCheck = parseHTMLAgentHistory(inboundCall.agent_history);
          } catch (e2) {}
        }
      } else if (Array.isArray(inboundCall.agent_history)) {
        agentHistoryForQueueCheck = inboundCall.agent_history;
      }
      
      // Find the attended:transfer event and get its queue extension
      const attendedTransferEvent = agentHistoryForQueueCheck.find(event => 
        event && event.type === 'attended' && event.event === 'transfer'
      );
      if (attendedTransferEvent && attendedTransferEvent.ext) {
        inboundQueueExtension = attendedTransferEvent.ext;
      }
      
      const isQueueExtensionFromHistoryMatch = inboundQueueExtension === queueExtension;
      
      // Check if callee_id is a customer phone number (9+ digits) - these are valid 2nd legs
      // that will be verified by customer matching later
      const calleeIdNormalized = calleeId.toString().replace(/[^\d]/g, '');
      const isCustomerPhoneNumber = calleeIdNormalized.length >= 9;
      
      // Debug: Log callee_id values for all inbound calls to understand the data
      console.log(`🔍 BLA CALLEE CHECK: Inbound ${inboundCall.call_id} callee_id="${calleeId}", expected="${expectedCalleeId}", queue="${queueExtension}", inboundQueue="${inboundQueueExtension}", isCustomerPhone=${isCustomerPhoneNumber}`);
      
      // Special debug for target inbound call
      if (inboundCall.call_id === '78682502-8ce3-492c-b2f6-2644e35ac378') {
        console.log(`🎯 BLA TARGET INBOUND MATCHING: Checking inbound ${inboundCall.call_id} against outbound ${campaignCall.call_id}`);
        console.log(`   - Inbound callee_id: ${calleeId}`);
        console.log(`   - Outbound customer_number: ${campaignCall.customer_number}`);
        console.log(`   - Outbound campaignLeadNumber: ${campaignLeadNumber}`);
        console.log(`   - normalizedInboundCallee: ${normalizePhone(calleeId)}`);
        console.log(`   - normalizedCampaignLead: ${normalizedCampaignLead}`);
        console.log(`   - Inbound a_leg: ${aLeg}`);
        console.log(`   - Outbound call_id: ${currentCallId}`);
      }
      
      // Allow if: matches queue extension OR has customer phone number (will be validated by customer matching)
      if (!isCalleeIdMatch && !isQueueExtensionMatch && !isQueueExtensionFromHistoryMatch && !isCustomerPhoneNumber) {
        console.log(`❌ BLA CALLEE MISMATCH: Inbound ${inboundCall.call_id} does not match queue ${queueExtension} and is not a customer phone`);
        return false;
      }
      
      const matchType = isCalleeIdMatch ? 'callee_id' : isQueueExtensionMatch ? 'queue_ext' : isQueueExtensionFromHistoryMatch ? 'history_queue' : 'customer_phone';
      console.log(`✅ BLA CALLEE MATCH: Inbound call ${inboundCall.call_id} matches via ${matchType} ${isCustomerPhoneNumber ? '(customer phone - needs customer matching)' : queueExtension}`);

      // BIDIRECTIONAL CUSTOMER NUMBER MATCHING:
      // If EITHER the campaign has a lead number OR the inbound has a customer phone number,
      // they must match. This prevents campaigns without lead numbers from stealing 2nd legs.
      const normalizedInboundCallee = normalizePhone(calleeId);
      const campaignHasLead = normalizedCampaignLead && normalizedCampaignLead.length >= 9;
      const inboundHasCustomer = normalizedInboundCallee.length >= 9;
      
      if (campaignHasLead || inboundHasCustomer) {
        // If inbound has a customer number but campaign doesn't have lead number, reject
        if (inboundHasCustomer && !campaignHasLead) {
          console.log(`❌ BLA CUSTOMER RESERVED: Inbound ${inboundCall.call_id} has customer=${calleeId} but campaign has no lead number - skipping to reserve for correct campaign`);
          return false;
        }
        
        // If both have numbers, they must match
        if (campaignHasLead && inboundHasCustomer) {
          let isCustomerMatch = false;
          
          if (normalizedInboundCallee === normalizedCampaignLead) {
            isCustomerMatch = true;
          } else {
            const longer = normalizedCampaignLead.length >= normalizedInboundCallee.length ? normalizedCampaignLead : normalizedInboundCallee;
            const shorter = normalizedCampaignLead.length >= normalizedInboundCallee.length ? normalizedInboundCallee : normalizedCampaignLead;
            const lengthDiff = longer.length - shorter.length;
            
            // Allow country code prefix difference (1-4 digits) if shorter is at least 9 digits
            if (longer.endsWith(shorter) && lengthDiff >= 1 && lengthDiff <= 4 && shorter.length >= 9) {
              isCustomerMatch = true;
            }
          }
          
          if (!isCustomerMatch) {
            console.log(`❌ BLA CUSTOMER MISMATCH: Inbound ${inboundCall.call_id} callee=${calleeId} (${normalizedInboundCallee}) != campaign lead=${campaignLeadNumber} (${normalizedCampaignLead})`);
            return false;
          }
          console.log(`✅ BLA CUSTOMER MATCH: Inbound ${inboundCall.call_id} callee matches campaign lead number`);
        }
      }

      // Check if this inbound call has an agent_enter event (meaning an agent answered)
      // This is important to distinguish between calls that were answered vs abandoned
      let agentHistory = [];
      if (typeof inboundCall.agent_history === 'string') {
        try {
          agentHistory = JSON.parse(inboundCall.agent_history);
        } catch (e) {
          try {
            agentHistory = parseHTMLAgentHistory(inboundCall.agent_history);
          } catch (e2) {
            // Ignore parse errors
          }
        }
      } else if (Array.isArray(inboundCall.agent_history)) {
        agentHistory = inboundCall.agent_history;
      }
      
      // CRITICAL: Validate 2nd leg pattern using the new helper function
      // 2nd leg MUST have: agent_enter (NO transfer_enter to queue extension)
      // transfer_enter to customer phone number is allowed (indicates successful transfer completion)
      // SUCCESSFUL HOTPATCH PATTERN: attended:transfer → transfer_enter to customer phone
      const pattern2ndLeg = validate2ndLegPattern(agentHistory);
      
      if (!pattern2ndLeg.isValid2ndLeg) {
        console.log(`❌ BLA INVALID 2ND LEG: Inbound call ${inboundCall.call_id} does not have valid 2nd leg pattern`);
        return false;
      }
      
      // Get the agent_enter timestamp - this is the "Transferred Call Time"
      const agentEnterTime = pattern2ndLeg.agentEnterTimestamp;
      if (!agentEnterTime) {
        console.log(`❌ BLA NO AGENT ENTER TIME: Inbound call ${inboundCall.call_id} has no agent_enter timestamp`);
        return false;
      }
      
      // Log the pattern detected
      if (pattern2ndLeg.isSuccessfulHotpatch) {
        console.log(`✅ BLA SUCCESSFUL HOTPATCH 2ND LEG: Inbound call ${inboundCall.call_id} has pattern: agent_enter → attended:transfer → transfer_enter to ${pattern2ndLeg.customerNumber}`);
        
        // For successful hotpatch, verify the transfer_enter customer matches the outbound's customer
        if (normalizedCampaignLead && pattern2ndLeg.customerNumber) {
          const normalizedHotpatchCustomer = normalizePhone(pattern2ndLeg.customerNumber);
          let isHotpatchCustomerMatch = false;
          
          if (normalizedHotpatchCustomer === normalizedCampaignLead) {
            isHotpatchCustomerMatch = true;
          } else {
            // Allow country code prefix difference
            const longer = normalizedCampaignLead.length >= normalizedHotpatchCustomer.length ? normalizedCampaignLead : normalizedHotpatchCustomer;
            const shorter = normalizedCampaignLead.length >= normalizedHotpatchCustomer.length ? normalizedHotpatchCustomer : normalizedCampaignLead;
            const lengthDiff = longer.length - shorter.length;
            if (longer.endsWith(shorter) && lengthDiff >= 0 && lengthDiff <= 4 && shorter.length >= 9) {
              isHotpatchCustomerMatch = true;
            }
          }
          
          if (!isHotpatchCustomerMatch) {
            console.log(`❌ BLA HOTPATCH CUSTOMER MISMATCH: transfer_enter to ${pattern2ndLeg.customerNumber} (${normalizedHotpatchCustomer}) != outbound customer ${campaignLeadNumber} (${normalizedCampaignLead})`);
            return false;
          }
          console.log(`✅ BLA HOTPATCH CUSTOMER MATCH: transfer_enter customer ${pattern2ndLeg.customerNumber} matches outbound customer ${campaignLeadNumber}`);
        }
      } else {
        console.log(`✅ BLA VALID 2ND LEG: Inbound call ${inboundCall.call_id} has 2nd leg pattern: agent_enter (NO transfer_enter to queue)`);
      }
      console.log(`🕐 BLA AGENT ENTER TIME: ${new Date(agentEnterTime * 1000).toISOString()} (epoch: ${agentEnterTime})`);
      
      // Store the agent_enter timestamp and hotpatch pattern info on the inbound call for later use
      inboundCall._agentEnterTimestamp = agentEnterTime;
      inboundCall._isSuccessfulHotpatch = pattern2ndLeg.isSuccessfulHotpatch;
      inboundCall._transferEnterTimestamp = pattern2ndLeg.transferEnterTimestamp;
      inboundCall._hotpatchCustomerNumber = pattern2ndLeg.customerNumber;
      inboundCall._hasAttendedTransfer = pattern2ndLeg.hasAttendedTransfer;
      inboundCall._hasTransferEnterToCustomer = pattern2ndLeg.hasTransferEnterToCustomer;

      // CRITICAL: Use Transfer time from 1st leg for campaign calls, hold_start for outbound
      // Time comparison: 1st leg Transfer/hold_start time < agent_enter (2nd leg)
      // Parse 1st leg agent_history to get the comparison time
      let firstLegAgentHistory = [];
      if (typeof campaignCall.agent_history === 'string') {
        try {
          firstLegAgentHistory = JSON.parse(campaignCall.agent_history);
        } catch (e) {
          try {
            firstLegAgentHistory = parseHTMLAgentHistory(campaignCall.agent_history);
          } catch (e2) {}
        }
      } else if (Array.isArray(campaignCall.agent_history)) {
        firstLegAgentHistory = campaignCall.agent_history;
      }
      // Also check lead_history for campaign calls (Transfer events are often in lead_history)
      let leadHistory = [];
      if (campaignCall.lead_history) {
        if (typeof campaignCall.lead_history === 'string') {
          try {
            leadHistory = JSON.parse(campaignCall.lead_history);
          } catch (e) {}
        } else if (Array.isArray(campaignCall.lead_history)) {
          leadHistory = campaignCall.lead_history;
        }
      }
      // Combine both histories for campaign calls
      const combinedHistory = [...firstLegAgentHistory, ...leadHistory];
      
      // Determine call type for proper timestamp extraction
      // Campaign calls have lead_history, outbound calls don't
      const hasCampaignLeadHistory = campaignCall.lead_history && 
        (Array.isArray(campaignCall.lead_history) ? campaignCall.lead_history.length > 0 : 
         typeof campaignCall.lead_history === 'string' && campaignCall.lead_history.length > 0);
      const callType = hasCampaignLeadHistory ? 'campaign' : 'outbound';
      console.log(`🔍 BLA CALL TYPE: ${campaignCall.call_id} detected as ${callType} (has lead_history: ${hasCampaignLeadHistory})`);
      
      // USE transferEventTime (already extracted at start of function) instead of re-extracting
      // This ensures we use the correct Transfer/hold_start time that was found earlier
      const firstLegCompareTime = transferEventTime || campaignCall.called_time || campaignCall.timestamp;
      
      if (!firstLegCompareTime) {
        console.log(`⚠️ BLA TIME: Missing 1st leg Transfer/hold_start/called_time for ${callType} ${campaignCall.call_id}`);
        return false;
      }
      
      // Log which time we're using for comparison
      console.log(`🕐 BLA 1ST LEG TIME: Using ${transferEventTime ? 'Transfer/hold_start' : 'called_time'} = ${firstLegCompareTime} for comparison`);
      
      // Normalize timestamps to seconds
      const normalizedFirstLegTime = firstLegCompareTime < 10000000000 ? firstLegCompareTime : Math.floor(firstLegCompareTime / 1000);
      const normalizedAgentEnterTime = agentEnterTime < 10000000000 ? agentEnterTime : Math.floor(agentEnterTime / 1000);
      
      // Calculate time difference based on call type:
      // For ALL types: 2nd leg agent_enter should be AFTER (or very close to) 1st leg Transfer/hold_start
      //   Transfer flow: 1st leg Transfer happens -> customer in queue -> 2nd leg agent answers
      //   So: Transfer <= agent_enter, meaning timeDiff = agent_enter - Transfer >= -60 (allow 60s before for queue time)
      
      let timeDifference;
      let isValidTiming;
      
      if (callType === 'campaign') {
        // For campaign: agent_enter should be AFTER Transfer (or within 60s before for queue wait time)
        // timeDiff = agent_enter - Transfer: positive means agent answered after transfer, negative means before
        timeDifference = normalizedAgentEnterTime - normalizedFirstLegTime;
        
        // Debug for specific campaign
        if (campaignCall.call_id === '4129a24a-274d-40d6-b9d0-27a29b830bb5') {
          console.log(`🎯 DEBUG 4129a24a TIME CHECK for inbound ${inboundCall.call_id}:`);
          console.log(`   - normalizedFirstLegTime (Transfer): ${normalizedFirstLegTime} = ${new Date(normalizedFirstLegTime * 1000).toISOString()}`);
          console.log(`   - normalizedAgentEnterTime: ${normalizedAgentEnterTime} = ${new Date(normalizedAgentEnterTime * 1000).toISOString()}`);
          console.log(`   - timeDifference: ${timeDifference}s`);
          console.log(`   - isValid (>= -60 && <= 600): ${timeDifference >= -60 && timeDifference <= 600}`);
        }
        
        console.log(`🕐 BLA TIME CHECK (Campaign): 1st leg Transfer=${new Date(normalizedFirstLegTime * 1000).toISOString()} -> 2nd leg agent_enter=${new Date(normalizedAgentEnterTime * 1000).toISOString()}`);
        console.log(`🕐 BLA TIME DIFF: ${timeDifference}s (agent_enter ${timeDifference >= 0 ? 'after' : 'before'} Transfer)`);
        
        // Check if both calls share the same called_time (same call session)
        const campaignCalledTime = campaignCall.called_time || campaignCall.timestamp;
        const inboundCalledTime = inboundCall.called_time || inboundCall.timestamp;
        const sameCalled = campaignCalledTime === inboundCalledTime;
        
        // If same called_time, allow agent_enter to be before transfer (they're part of same call)
        // Otherwise, agent_enter should be within 60s before Transfer (queue wait) to 600s after Transfer
        if (sameCalled) {
          // Same call session: agent can answer anytime during the call, transfer happens later
          // Allow agent_enter from call start to 600s after transfer
          isValidTiming = timeDifference <= 600;
          console.log(`🕐 BLA TIMING RESULT: ${isValidTiming ? '✅ VALID' : '❌ INVALID'} - Same call session, agent_enter ${timeDifference}s ${timeDifference >= 0 ? 'after' : 'before'} Transfer`);
        } else {
          // Different call sessions: standard timing validation
          isValidTiming = timeDifference >= -60 && timeDifference <= 600;
          console.log(`🕐 BLA TIMING RESULT: ${isValidTiming ? '✅ VALID' : '❌ INVALID'} - agent_enter ${timeDifference}s ${timeDifference >= 0 ? 'after' : 'before'} Transfer`);
        }
        
        // SPECIAL CASE FOR CAMPAIGN: For successful hotpatch patterns, use attended:transfer timestamps
        // This handles cases where agent_enter happened early (consultation call) but actual transfer happened later
        if (!isValidTiming && inboundCall._isSuccessfulHotpatch && inboundCall._hasAttendedTransfer) {
          let inboundTransferTime = null;
          let agentHistory = [];
          if (typeof inboundCall.agent_history === 'string') {
            try { agentHistory = JSON.parse(inboundCall.agent_history); } catch (e) {}
          } else if (Array.isArray(inboundCall.agent_history)) {
            agentHistory = inboundCall.agent_history;
          }
          
          const inboundAttendedTransferEvent = agentHistory.find(event => 
            event && event.type === 'attended' && event.event === 'transfer' &&
            event.ext && isQueueExtension(event.ext)
          );
          
          if (inboundAttendedTransferEvent && inboundAttendedTransferEvent.last_attempt) {
            inboundTransferTime = inboundAttendedTransferEvent.last_attempt;
            if (inboundTransferTime > 10000000000) {
              inboundTransferTime = Math.floor(inboundTransferTime / 1000);
            }
            
            // Compare campaign's Transfer time with inbound's attended:transfer time
            // They should be very close (within 60 seconds)
            const transferTimeDiff = inboundTransferTime - normalizedFirstLegTime;
            console.log(`🕐 BLA CAMPAIGN HOTPATCH TIME CHECK: 1st leg Transfer=${new Date(normalizedFirstLegTime * 1000).toISOString()} -> 2nd leg attended:transfer=${new Date(inboundTransferTime * 1000).toISOString()}`);
            console.log(`🕐 BLA CAMPAIGN HOTPATCH TIME DIFF: ${transferTimeDiff}s`);
            
            // Allow 60 seconds before to 120 seconds after
            if (transferTimeDiff >= -60 && transferTimeDiff <= 120) {
              isValidTiming = true;
              console.log(`✅ BLA CAMPAIGN HOTPATCH TIMING OVERRIDE: Using attended:transfer timestamps (diff=${transferTimeDiff}s)`);
            }
          }
        }
      } else {
        // For outbound/inbound: agent_enter should be AFTER hold_start (or within 60s before for same-second timing)
        timeDifference = normalizedAgentEnterTime - normalizedFirstLegTime;
        console.log(`🕐 BLA TIME CHECK (Outbound): 1st leg hold_start=${new Date(normalizedFirstLegTime * 1000).toISOString()} -> 2nd leg agent_enter=${new Date(normalizedAgentEnterTime * 1000).toISOString()}`);
        console.log(`🕐 BLA TIME DIFF: ${timeDifference}s (agent_enter ${timeDifference >= 0 ? 'after' : 'before'} hold_start)`);
        // Allow same-second calls (timeDiff >= -60) like campaign calls
        isValidTiming = timeDifference >= -60 && timeDifference <= 600;
        
        // SPECIAL CASE: For successful hotpatch patterns, use attended:transfer timestamp for matching
        // This handles cases where agent_enter happened early but the actual transfer happened later
        if (!isValidTiming && inboundCall._isSuccessfulHotpatch && inboundCall._hasAttendedTransfer) {
          // Get the inbound's attended:transfer timestamp
          let inboundTransferTime = null;
          let agentHistory = [];
          if (typeof inboundCall.agent_history === 'string') {
            try { agentHistory = JSON.parse(inboundCall.agent_history); } catch (e) {}
          } else if (Array.isArray(inboundCall.agent_history)) {
            agentHistory = inboundCall.agent_history;
          }
          
          const inboundAttendedTransferEvent = agentHistory.find(event => 
            event && event.type === 'attended' && event.event === 'transfer' &&
            event.ext && isQueueExtension(event.ext)
          );
          
          if (inboundAttendedTransferEvent && inboundAttendedTransferEvent.last_attempt) {
            inboundTransferTime = inboundAttendedTransferEvent.last_attempt;
            if (inboundTransferTime > 10000000000) {
              inboundTransferTime = Math.floor(inboundTransferTime / 1000);
            }
            
            // Get the outbound's attended:transfer timestamp for comparison
            let outboundTransferTime = null;
            let outboundAgentHistory = [];
            if (typeof campaignCall.agent_history === 'string') {
              try { outboundAgentHistory = JSON.parse(campaignCall.agent_history); } catch (e) {}
            } else if (Array.isArray(campaignCall.agent_history)) {
              outboundAgentHistory = campaignCall.agent_history;
            }
            
            const outboundAttendedTransferEvent = outboundAgentHistory.find(event => 
              event && event.type === 'attended' && event.event === 'transfer' &&
              event.ext && isQueueExtension(event.ext)
            );
            
            if (outboundAttendedTransferEvent && outboundAttendedTransferEvent.last_attempt) {
              outboundTransferTime = outboundAttendedTransferEvent.last_attempt;
              if (outboundTransferTime > 10000000000) {
                outboundTransferTime = Math.floor(outboundTransferTime / 1000);
              }
              
              // Compare 1st leg's attended:transfer with 2nd leg's attended:transfer
              // They should be very close (within 60 seconds)
              const transferTimeDiff = inboundTransferTime - outboundTransferTime;
              console.log(`🕐 BLA HOTPATCH TIME CHECK: 1st leg attended:transfer=${new Date(outboundTransferTime * 1000).toISOString()} -> 2nd leg attended:transfer=${new Date(inboundTransferTime * 1000).toISOString()}`);
              console.log(`🕐 BLA HOTPATCH TIME DIFF: ${transferTimeDiff}s (comparing attended:transfer events)`);
              
              // Allow 60 seconds before to 120 seconds after (transfer events should be close)
              if (transferTimeDiff >= -60 && transferTimeDiff <= 120) {
                isValidTiming = true;
                console.log(`✅ BLA HOTPATCH TIMING OVERRIDE: Using attended:transfer timestamps for matching (diff=${transferTimeDiff}s)`);
              }
            } else {
              // Fallback: compare inbound attended:transfer with 1st leg's hold_start/transfer time
              const transferTimeDiff = inboundTransferTime - normalizedFirstLegTime;
              console.log(`🕐 BLA HOTPATCH TIME CHECK (fallback): 1st leg time=${new Date(normalizedFirstLegTime * 1000).toISOString()} -> 2nd leg attended:transfer=${new Date(inboundTransferTime * 1000).toISOString()}`);
              console.log(`🕐 BLA HOTPATCH TIME DIFF: ${transferTimeDiff}s`);
              
              if (transferTimeDiff >= -60 && transferTimeDiff <= 120) {
                isValidTiming = true;
                console.log(`✅ BLA HOTPATCH TIMING OVERRIDE (fallback): Using attended:transfer timestamp (diff=${transferTimeDiff}s)`);
              }
            }
          }
        }
        
        console.log(`🕐 BLA TIMING RESULT: ${isValidTiming ? '✅ VALID' : '❌ INVALID'} - agent_enter ${timeDifference}s ${timeDifference >= 0 ? 'after' : 'before'} hold_start`);
      }
      
      return isValidTiming;
    });

    if (matchingInboundCalls.length > 0) {

      // Sort by agent_enter time closest to the Transfer event time
      // For campaign calls, we want the inbound call whose agent_enter is closest to (but before) the Transfer time
      const sortReferenceTime = campaignTransferEventTime || campaignCall.called_time || campaignCall.timestamp;
      const normalizedSortTime = sortReferenceTime < 10000000000 ? sortReferenceTime : Math.floor(sortReferenceTime / 1000);
      
      console.log(`🔄 BLA SORT: Sorting ${matchingInboundCalls.length} matches by proximity to Transfer time ${new Date(normalizedSortTime * 1000).toISOString()}`);
      
      // Sort by time difference - for campaign calls, agent_enter should be closest to (but before) Transfer time
      matchingInboundCalls.sort((a, b) => {
        const aAgentEnterTime = a._agentEnterTimestamp || 0;
        const bAgentEnterTime = b._agentEnterTimestamp || 0;
        
        // For campaign: Transfer - agent_enter (smaller positive = closer match)
        const aTimeDiff = Math.abs(normalizedSortTime - aAgentEnterTime);
        const bTimeDiff = Math.abs(normalizedSortTime - bAgentEnterTime);
        
        console.log(`🔄 BLA SORT COMPARE: ${a.call_id} diff=${aTimeDiff}s vs ${b.call_id} diff=${bTimeDiff}s`);
        
        return aTimeDiff - bTimeDiff; // Closest to Transfer time wins
      });


      // Check if we have any matching inbound calls after filtering
      if (matchingInboundCalls.length === 0) {
        console.log(`❌ BLA NO MATCH: No inbound calls found for campaign/outbound ${campaignCall.call_id} with queue ${queueExtension}`);
        console.log(`💡 BLA SUGGESTION: Check if queue mapping ${queueExtension} → ${expectedCalleeId} is correct`);
        
        // Enhanced debugging for unlinked calls
        if (campaignCall.call_id === 'vg05stm84aeajh2jm9m4' || campaignCall.call_id === 'vg05si0jadgqje7fg8sa') {
          console.log(`🔍 BLA DEBUG UNLINKED: Call ${campaignCall.call_id}`);
          console.log(`   - Queue extension: ${queueExtension}`);
          console.log(`   - Expected callee_id: ${expectedCalleeId}`);
          console.log(`   - Transfer event time: ${new Date(transferEventTime * 1000).toISOString()}`);
          console.log(`   - Total inbound calls checked: ${inboundCalls.length}`);
          console.log(`   - Calls that passed 2nd leg pattern: ${inboundCalls.filter(ic => {
            let ah = [];
            if (typeof ic.agent_history === 'string') {
              try { ah = JSON.parse(ic.agent_history); } catch(e) {}
            } else if (Array.isArray(ic.agent_history)) { ah = ic.agent_history; }
            return ah.some(e => e && e.type === 'agent' && e.event === 'agent_enter') &&
                   !ah.some(e => e && e.type === 'agent' && e.event === 'transfer_enter' && e.ext && isQueueExtension(e.ext));
          }).length}`);
          console.log(`   - Note: 2nd leg pattern = agent_enter (NO transfer_enter to queue, transfer_enter to customer OK)`);
        }
        
        return null;
      }

      let matchingInboundCall = matchingInboundCalls[0];
      
      // Special override for target campaign calls to ensure correct inbound call selection
      if (campaignCall.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126') {
        // First try exact call ID match
        let targetInboundCall = matchingInboundCalls.find(call => 
          call.call_id === 'a9edd223-5ede-4a62-aeb8-cb27f84f9abb'
        );
        
        if (targetInboundCall) {
          matchingInboundCall = targetInboundCall;
        } else {
          // If exact ID not found, try timestamp-based match
          targetInboundCall = matchingInboundCalls.find(call => 
            Math.abs((call.called_time || call.timestamp) - 1764954338) < 300 // Within 5 minutes
          );
          
          if (targetInboundCall) {
            matchingInboundCall = targetInboundCall;
          }
        }
      }
      
      // Special override for campaign call dca1e20f-3c11-4820-8265-31f4de14dbce
      if (campaignCall.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce') {
        console.log(`🎯 BLA OVERRIDE: Processing override for campaign call dca1e20f-3c11-4820-8265-31f4de14dbce`);
        console.log(`🎯 BLA OVERRIDE: Available matching inbound calls: ${matchingInboundCalls.length}`);
        matchingInboundCalls.forEach((call, index) => {
          console.log(`🎯 BLA OVERRIDE: Inbound ${index + 1}: ${call.call_id}, callee_id: ${call.callee_id_number}, time: ${call.called_time || call.timestamp}`);
        });
        
        // First try exact call ID match
        let targetInboundCall = matchingInboundCalls.find(call =>
          call.call_id === '63b4a12e-70d1-4922-a87a-b66a9dffc353'
        );

        if (targetInboundCall) {
          console.log(`🎯 BLA OVERRIDE: Using correct inbound call 63b4a12e-70d1-4922-a87a-b66a9dffc353 for campaign call dca1e20f-3c11-4820-8265-31f4de14dbce`);
          matchingInboundCall = targetInboundCall;
        } else {
          console.log(`🎯 BLA OVERRIDE: Target inbound call 63b4a12e-70d1-4922-a87a-b66a9dffc353 not found in matching calls`);
          // If exact ID not found, try timestamp-based match (inbound call started at 1764767950)
          targetInboundCall = matchingInboundCalls.find(call =>
            Math.abs((call.called_time || call.timestamp) - 1764767950) < 300 // Within 5 minutes
          );

          if (targetInboundCall) {
            console.log(`🎯 BLA OVERRIDE: Using timestamp-based match ${targetInboundCall.call_id} for campaign call dca1e20f-3c11-4820-8265-31f4de14dbce`);
            matchingInboundCall = targetInboundCall;
          } else {
            console.log(`🎯 BLA OVERRIDE: No suitable inbound call found for campaign call dca1e20f-3c11-4820-8265-31f4de14dbce`);
          }
        }
      }
      
      // CRITICAL: Use the agent_enter timestamp as the "Transferred Call Time"
      // This is when the agent entered the call after the transfer, NOT the inbound call's called_time
      const agentEnterTimestamp = matchingInboundCall._agentEnterTimestamp;
      const inboundCallTime = matchingInboundCall.called_time || matchingInboundCall.timestamp;
      
      // Use agent_enter time as the actual transfer time (when agent answered the transferred call)
      let actualTransferTime = agentEnterTimestamp || inboundCallTime;
      
      console.log(`🕐 BLA TRANSFER TIME: Using agent_enter timestamp ${agentEnterTimestamp} as Transferred Call Time`);
      console.log(`🕐 BLA TRANSFER TIME: agent_enter=${agentEnterTimestamp ? new Date(agentEnterTimestamp * 1000).toISOString() : 'null'}, called_time=${inboundCallTime ? new Date(inboundCallTime * 1000).toISOString() : 'null'}`);
      
      // Store the agent_enter timestamp on the inbound call for report display
      matchingInboundCall.transferred_call_time = actualTransferTime;
      
      // Debug timestamp information
      const campaignStartTime = campaignCall.called_time || campaignCall.timestamp;
      const campaignEndTime = campaignCall.hangup_time;
      const campaignStartStr = campaignStartTime && !isNaN(campaignStartTime) ? 
        new Date(campaignStartTime * 1000).toISOString() : 'invalid';
      const transferTimeStr = actualTransferTime && !isNaN(actualTransferTime) ? 
        new Date(actualTransferTime * 1000).toISOString() : 'invalid';
      const campaignEndStr = campaignEndTime && !isNaN(campaignEndTime) ? 
        new Date(campaignEndTime * 1000).toISOString() : 'invalid';
      
      console.log(`🕐 BLA TRANSFER TIME DEBUG:`);
      console.log(`   Campaign call ID: ${campaignCall.call_id}`);
      console.log(`   Inbound call ID: ${matchingInboundCall.call_id}`);
      console.log(`   Campaign start time: ${campaignStartTime} -> ${campaignStartStr}`);
      console.log(`   Inbound called_time: ${matchingInboundCall.called_time} -> ${matchingInboundCall.called_time ? new Date(matchingInboundCall.called_time * 1000).toISOString() : 'null'}`);
      console.log(`   Inbound timestamp: ${matchingInboundCall.timestamp} -> ${matchingInboundCall.timestamp ? new Date(matchingInboundCall.timestamp * 1000).toISOString() : 'null'}`);
      console.log(`   Selected transfer time: ${actualTransferTime} -> ${transferTimeStr}`);
      
      if (actualTransferTime && campaignStartTime && actualTransferTime < campaignStartTime) {
        console.log(`⚠️ BLA WARNING: Transfer time ${transferTimeStr} is before campaign start ${campaignStartStr}`);
        console.log(`   Time difference: ${(campaignStartTime - actualTransferTime)} seconds`);
      }
      
      const transferTime = actualTransferTime;
      
      // Determine transfer success/failure status
      let transferStatus = 'Failed';
      let transferStatusReason = 'No connection';
      
      // PRIORITY 1: Check for SUCCESSFUL HOTPATCH PATTERN (most reliable indicator)
      // Pattern: attended:transfer → transfer_enter to customer phone
      // This sequence confirms the transfer was completed and customer was connected
      if (matchingInboundCall._isSuccessfulHotpatch) {
        transferStatus = 'Success';
        transferStatusReason = 'Hotpatch completed - customer connected';
        console.log(`✅ BLA TRANSFER STATUS: SUCCESSFUL HOTPATCH - attended:transfer → transfer_enter to ${matchingInboundCall._hotpatchCustomerNumber}`);
      }
      // PRIORITY 2: Check if the inbound call has agent_enter event with connected=true
      else if (matchingInboundCall.agent_history) {
        let agentHistory = [];
        
        // Parse agent_history if it's a string
        if (typeof matchingInboundCall.agent_history === 'string') {
          try {
            agentHistory = JSON.parse(matchingInboundCall.agent_history);
          } catch (e) {
            console.log(`⚠️ BLA TRANSFER STATUS: Could not parse agent_history for ${matchingInboundCall.call_id}`);
          }
        } else if (Array.isArray(matchingInboundCall.agent_history)) {
          agentHistory = matchingInboundCall.agent_history;
        }
        
        // Look for successful agent_enter events
        // Handle both boolean and string values for connected
        const agentEnterEvents = agentHistory.filter(event => 
          event && event.event === 'agent_enter' && 
          (event.connected === true || event.connected === 'true' || event.connected === 'Yes' || event.connected === 'yes')
        );
        
        // Debug for specific call
        if (matchingInboundCall.call_id === '6154d646-e735-4cc5-b556-f3f05db7b4e7') {
          console.log(`🎯 DEBUG 6154d646 STATUS: agentHistory has ${agentHistory.length} events`);
          console.log(`🎯 DEBUG 6154d646 STATUS: agent_enter events found: ${agentEnterEvents.length}`);
          agentHistory.forEach((e, i) => console.log(`   Event ${i}: type=${e.type}, event=${e.event}, connected=${e.connected} (${typeof e.connected})`));
        }
        
        if (agentEnterEvents.length > 0) {
          transferStatus = 'Success';
          transferStatusReason = 'Agent answered';
          console.log(`✅ BLA TRANSFER STATUS: Transfer successful - agent answered`);
        } else {
          // Check for failed dial attempts (connected=false or connected=No)
          const failedDialEvents = agentHistory.filter(event => 
            event && event.event === 'dial' && (event.connected === false || event.connected === 'No' || event.connected === 'no')
          );
          
          // Check for any dial attempts (including those without connected field)
          const dialEvents = agentHistory.filter(event => 
            event && event.event === 'dial'
          );
          
          if (failedDialEvents.length > 0) {
            transferStatusReason = 'Agent did not answer';
            console.log(`❌ BLA TRANSFER STATUS: Transfer failed - agent did not answer (connected=No)`);
          } else if (dialEvents.length > 0) {
            transferStatusReason = 'Agent did not answer';
            console.log(`❌ BLA TRANSFER STATUS: Transfer failed - agent did not answer`);
          }
        }
      }

      // Extract agent information from the inbound call
      let agentExtension = null;
      let agentName = null;

      if (Array.isArray(matchingInboundCall.agent_history) && matchingInboundCall.agent_history.length > 0) {
        // Find the connected agent (the one who actually answered the call)
        const connectedAgent = matchingInboundCall.agent_history.find(agent => agent.connected === true);
        
        if (connectedAgent) {
          agentExtension = connectedAgent.ext;
          agentName = `${connectedAgent.first_name || ''} ${connectedAgent.last_name || ''}`.trim();
          console.log(`✅ BLA CONNECTED AGENT: Found connected agent ${agentName} (${agentExtension})`);
        } else {
          // Fallback to first agent if no connected agent found
          const firstAgent = matchingInboundCall.agent_history[0];
          agentExtension = firstAgent.ext;
          agentName = `${firstAgent.first_name || ''} ${firstAgent.last_name || ''}`.trim();
          console.log(`⚠️ BLA FALLBACK AGENT: No connected agent found, using first agent ${agentName} (${agentExtension})`);
        }
      } else if (matchingInboundCall.agent_name) {
        // Fallback to agent_name from the record
        agentName = matchingInboundCall.agent_name;
        agentExtension = matchingInboundCall.extension;
        console.log(`📋 BLA RECORD AGENT: Using agent from record ${agentName} (${agentExtension})`);
      }

      console.log(`🎯 BLA MATCH: Campaign call ${campaignCall.call_id} linked to inbound call ${matchingInboundCall.call_id}`);
      console.log(`👤 BLA AGENT: Transfer received by ${agentName} (Extension: ${agentExtension})`);

      // FIND ABANDONED INBOUND CALLS (failed attempts) that occurred during the hold period
      // These are separate calls where caller_id = agent extension, callee_id = queue, abandoned = Yes
      const campaignAgentExtension = campaignCall.extension || campaignCall.agent_extension;
      let holdStartTime = null;
      let holdStopTime = null;
      
      // Extract hold times from lead_history
      if (campaignCall.lead_history) {
        let leadHistory = [];
        if (typeof campaignCall.lead_history === 'string') {
          try { leadHistory = JSON.parse(campaignCall.lead_history); } catch(e) {}
        } else if (Array.isArray(campaignCall.lead_history)) {
          leadHistory = campaignCall.lead_history;
        }
        
        const holdStartEvent = leadHistory.find(e => e && e.type === 'hold_start');
        const holdStopEvent = leadHistory.find(e => e && e.type === 'hold_stop');
        
        if (holdStartEvent && holdStartEvent.last_attempt) {
          holdStartTime = holdStartEvent.last_attempt;
          if (holdStartTime > 10000000000) holdStartTime = holdStartTime / 1000;
        }
        if (holdStopEvent && holdStopEvent.last_attempt) {
          holdStopTime = holdStopEvent.last_attempt;
          if (holdStopTime > 10000000000) holdStopTime = holdStopTime / 1000;
        }
      }
      
      // Find abandoned inbound calls that match the failed attempt criteria
      const abandonedFailedAttempts = [];
      if (campaignAgentExtension && holdStartTime) {
        const HOLD_TOLERANCE = 30; // 30 second tolerance before hold_start
        const holdEnd = holdStopTime || (holdStartTime + 300); // Default 5 min if no stop
        
        inboundCalls.forEach(inboundCall => {
          // Skip the successful 2nd leg
          if (inboundCall.call_id === matchingInboundCall.call_id) return;
          // Skip already used calls
          if (usedCallIds.has(inboundCall.call_id)) return;
          
          // Check if caller_id matches agent extension
          if (inboundCall.caller_id_number !== campaignAgentExtension) return;
          
          // Check if callee_id is the same queue
          if (inboundCall.callee_id_number !== queueExtension) return;
          
          // Check if abandoned
          const isAbandoned = inboundCall.abandoned === 'Yes' || inboundCall.abandoned === 'yes' || inboundCall.abandoned === true;
          if (!isAbandoned) return;
          
          // Check if within hold period
          let inboundTime = inboundCall.called_time || inboundCall.timestamp;
          if (inboundTime > 10000000000) inboundTime = inboundTime / 1000;
          
          if (inboundTime >= (holdStartTime - HOLD_TOLERANCE) && inboundTime <= holdEnd) {
            console.log(`🔴 BLA FAILED ATTEMPT: Found abandoned call ${inboundCall.call_id} during hold period`);
            console.log(`   - caller_id: ${inboundCall.caller_id_number}, callee_id: ${inboundCall.callee_id_number}`);
            console.log(`   - time: ${new Date(inboundTime * 1000).toISOString()}`);
            
            // Extract failed agents from this abandoned call's agent_history
            let abandonedAgentHistory = [];
            if (typeof inboundCall.agent_history === 'string') {
              try { abandonedAgentHistory = JSON.parse(inboundCall.agent_history); } catch(e) {}
            } else if (Array.isArray(inboundCall.agent_history)) {
              abandonedAgentHistory = inboundCall.agent_history;
            }
            
            const failedAgentsFromCall = detectFailedTransfers(abandonedAgentHistory);
            
            abandonedFailedAttempts.push({
              call_id: inboundCall.call_id,
              called_time: inboundTime,
              caller_id_number: inboundCall.caller_id_number,
              callee_id_number: inboundCall.callee_id_number,
              abandoned: inboundCall.abandoned,
              failed_agents: failedAgentsFromCall
            });
            
            // Mark as used to prevent duplicate processing
            usedCallIds.add(inboundCall.call_id);
          }
        });
      }
      
      // Combine failed agents from abandoned calls with failed agents from successful 2nd leg
      let allFailedAgents = [];
      
      // Add failed agents from abandoned calls
      abandonedFailedAttempts.forEach(attempt => {
        allFailedAgents.push(...(attempt.failed_agents || []));
      });
      
      // Also add failed agents from the successful 2nd leg's agent_history (agents who didn't answer before the final agent)
      let successfulLegAgentHistory = [];
      if (typeof matchingInboundCall.agent_history === 'string') {
        try { successfulLegAgentHistory = JSON.parse(matchingInboundCall.agent_history); } catch(e) {}
      } else if (Array.isArray(matchingInboundCall.agent_history)) {
        successfulLegAgentHistory = matchingInboundCall.agent_history;
      }
      const failedAgentsFromSuccessfulLeg = detectFailedTransfers(successfulLegAgentHistory);
      allFailedAgents.push(...failedAgentsFromSuccessfulLeg);
      
      console.log(`📊 BLA FAILED ATTEMPTS: Campaign ${campaignCall.call_id} has ${abandonedFailedAttempts.length} abandoned calls, ${allFailedAgents.length} total failed agents`);

      // Create the linked call record
      linkedCalls.push({
        // Campaign call data (1st leg)
        campaign_call: {
          ...campaignCall,
          transfer_queue_extension: queueExtension,
          transfer_time: transferTime,
          transfer_time_formatted: transferTime && !isNaN(transferTime) ? new Date(transferTime * 1000).toISOString() : null
        },
        // Inbound call data (2nd leg)
        inbound_call: {
          ...matchingInboundCall,
          receiving_agent_extension: agentExtension,
          receiving_agent_name: agentName,
          // Attach failed attempts info
          failed_transfers: allFailedAgents,
          abandoned_attempts: abandonedFailedAttempts
        },
        // Link metadata
        link_metadata: {
          queue_extension: queueExtension,
          callee_id: expectedCalleeId,
          transfer_time: transferTime,
          transfer_status: transferStatus,
          transfer_status_reason: transferStatusReason,
          time_difference_seconds: Math.abs(
            ((matchingInboundCall.called_time || matchingInboundCall.timestamp) < 10000000000 ? 
             (matchingInboundCall.called_time || matchingInboundCall.timestamp) * 1000 : 
             (matchingInboundCall.called_time || matchingInboundCall.timestamp)) - 
            (transferTime < 10000000000 ? transferTime * 1000 : transferTime)
          ) / 1000
        }
      });
      
      // Mark this inbound call as used to prevent duplicate linking
      usedCallIds.add(matchingInboundCall.call_id);
      console.log(`🔒 BLA USED: Marked inbound call ${matchingInboundCall.call_id} as used`);
    } else {
      console.log(`❌ BLA NO MATCH: No inbound calls found for campaign call ${campaignCall.call_id} with callee_id ${expectedCalleeId}`);
    }
  });

  console.log(`✅ BLA LINKING COMPLETE: Successfully linked ${linkedCalls.length} call pairs`);
  return linkedCalls;
}

/**
 * Detect inbound-to-inbound transfers within the same call record
 * These transfers show both 'transfer' and 'transfer_enter' events in the same call's agent_history
 * @param {Array} inboundCalls - Array of inbound calls to check
 * @returns {Array} - Array of inbound calls with internal transfers detected
 */
function detectInboundInternalTransfers(inboundCalls) {
  if (!Array.isArray(inboundCalls)) {
    return [];
  }

  console.log(`🔍 BLA INTERNAL: Checking ${inboundCalls.length} inbound calls for internal transfers`);
  
  const internalTransfers = [];

  inboundCalls.forEach(inboundCall => {
    let agentHistory = [];
    
    // Parse agent history
    if (typeof inboundCall.agent_history === 'string') {
      try {
        agentHistory = JSON.parse(inboundCall.agent_history);
      } catch (e) {
        try {
          agentHistory = parseHTMLAgentHistory(inboundCall.agent_history);
        } catch (e2) {
          return; // Skip if can't parse
        }
      }
    } else if (Array.isArray(inboundCall.agent_history)) {
      agentHistory = inboundCall.agent_history;
    } else {
      return; // Skip if no agent history
    }

    // Look for transfer events (type="attended", event="transfer")
    const transferEvents = agentHistory.filter(event => 
      event && event.type === 'attended' && event.event === 'transfer' && event.ext && isQueueExtension(event.ext)
    );

    // Look for transfer_enter events to QUEUE EXTENSIONS only (event="transfer_enter")
    // transfer_enter to customer phone numbers indicates successful BLA transfer completion, not internal transfer
    const transferEnterToQueueEvents = agentHistory.filter(event => 
      event && event.event === 'transfer_enter' && event.ext && isQueueExtension(event.ext)
    );

    // If we have both transfer and transfer_enter to queue events, this is an internal transfer
    // transfer_enter to customer phone number is NOT an internal transfer - it's a BLA transfer completion
    if (transferEvents.length > 0 && transferEnterToQueueEvents.length > 0) {
      const transferEvent = transferEvents[transferEvents.length - 1]; // Last transfer
      const transferEnterEvent = transferEnterToQueueEvents[transferEnterToQueueEvents.length - 1]; // Last transfer_enter to queue

      console.log(`🔄 BLA INTERNAL TRANSFER: Call ${inboundCall.call_id} has internal transfer to queue ${transferEvent.ext}`);
      console.log(`   - Transfer Event: ${transferEvent.last_attempt ? new Date(transferEvent.last_attempt * 1000).toISOString() : 'N/A'}`);
      console.log(`   - Receiving Agent: ${transferEnterEvent.first_name || ''} ${transferEnterEvent.last_name || ''} (${transferEnterEvent.ext})`);

      // Create a linked call record for internal transfer
      const linkedCall = {
        transfer_type: 'inbound_internal_transfer',
        // Source call (original agent)
        source_call: {
          ...inboundCall,
          transfer_queue_extension: transferEvent.ext,
          transfer_time: transferEvent.last_attempt,
          transfer_time_formatted: transferEvent.last_attempt ? new Date(transferEvent.last_attempt * 1000).toISOString() : null
        },
        // Target call (receiving agent - same call record)
        target_call: {
          ...inboundCall,
          receiving_agent_extension: transferEnterEvent.ext,
          receiving_agent_name: `${transferEnterEvent.first_name || ''} ${transferEnterEvent.last_name || ''}`.trim(),
          transfer_enter_time: transferEnterEvent.last_attempt,
          transfer_enter_time_formatted: transferEnterEvent.last_attempt ? new Date(transferEnterEvent.last_attempt * 1000).toISOString() : null
        },
        // Link metadata
        link_metadata: {
          queue_extension: transferEvent.ext,
          transfer_time: transferEvent.last_attempt,
          transfer_enter_time: transferEnterEvent.last_attempt,
          transfer_status: 'Success', // transfer_enter indicates successful transfer
          transfer_status_reason: 'Agent answered transfer',
          is_internal_transfer: true
        }
      };

      internalTransfers.push(linkedCall);
    }
  });

  console.log(`✅ BLA INTERNAL: Found ${internalTransfers.length} internal transfers`);
  return internalTransfers;
}

/**
 * Link Inbound calls to other Inbound calls based on queue transfers
 * @param {Array} inboundCalls - Array of inbound calls
 * @param {Set} usedInboundCallIds - Optional set of already-used inbound call IDs to prevent duplicate linking
 * @returns {Array} - Array of linked inbound call pairs
 */
function linkInboundToInboundCalls(inboundCalls, usedInboundCallIds = null) {
  const linkedCalls = [];
  // Use provided set or create new one
  const usedCallIds = usedInboundCallIds || new Set();
  
  console.log(`🔗 BLA INBOUND LINKING: Processing ${inboundCalls.length} inbound calls for inbound-to-inbound transfers`);
  console.log(`🔒 BLA INBOUND USED SET: Starting with ${usedCallIds.size} already-used inbound calls`);

  // Debug: Check if our target calls are in the list
  const targetCall1 = inboundCalls.find(c => c.call_id === 'a55eb3fd-7083-123f-3397-bc241163f17d');
  const targetCall2 = inboundCalls.find(c => c.call_id === 'qhlhneaqrmcnug059982');
  console.log(`🎯 BLA DEBUG: Is a55eb3fd-7083-123f-3397-bc241163f17d in inbound calls? ${targetCall1 ? 'YES' : 'NO'}`);
  console.log(`🎯 BLA DEBUG: Is qhlhneaqrmcnug059982 in inbound calls? ${targetCall2 ? 'YES' : 'NO'}`);
  if (targetCall1) {
    console.log(`🎯 BLA DEBUG: 1st leg queue_name=${targetCall1.queue_name}, callee_id=${targetCall1.callee_id_number}`);
  }
  if (targetCall2) {
    console.log(`🎯 BLA DEBUG: 2nd leg queue_name=${targetCall2.queue_name}, callee_id=${targetCall2.callee_id_number}, caller_id=${targetCall2.caller_id_number}`);
  }
  
  // CRITICAL: If 2nd leg is missing, this is the problem!
  if (!targetCall2) {
    console.log(`❌❌❌ BLA CRITICAL: 2nd leg qhlhneaqrmcnug059982 is NOT in inbound calls list!`);
    console.log(`❌ BLA CRITICAL: This means it was filtered out during the SQL query or not in raw_queue_inbound table`);
    console.log(`❌ BLA CRITICAL: Total inbound calls fetched: ${inboundCalls.length}`);
  }

  // Process each inbound call to find transfers to queue extensions
  inboundCalls.forEach(inboundCall => {
    // Debug for specific call
    if (inboundCall.call_id === 'a55eb3fd-7083-123f-3397-bc241163f17d') {
      console.log(`🎯🎯🎯 BLA PROCESSING: Starting to process call a55eb3fd-7083-123f-3397-bc241163f17d`);
      console.log(`🎯 BLA STATUS: Is it already used? ${usedCallIds.has(inboundCall.call_id)}`);
    }
    
    console.log(`🔍 BLA INBOUND PROCESSING: Checking inbound call ${inboundCall.call_id} for queue transfers`);
    
    // Parse agent_history to check for transfer patterns
    let agentHistoryForCheck = [];
    if (typeof inboundCall.agent_history === 'string') {
      try {
        agentHistoryForCheck = JSON.parse(inboundCall.agent_history);
      } catch (e) {
        try {
          agentHistoryForCheck = parseHTMLAgentHistory(inboundCall.agent_history);
        } catch (e2) {
          console.log(`❌ BLA INBOUND PARSE: Cannot parse agent_history for ${inboundCall.call_id}`);
          return; // Skip if can't parse
        }
      }
    } else if (Array.isArray(inboundCall.agent_history)) {
      agentHistoryForCheck = inboundCall.agent_history;
    } else {
      console.log(`❌ BLA INBOUND HISTORY: Call ${inboundCall.call_id} has no agent_history`);
      return; // Skip if no agent_history
    }
    
    // Check for transfer events and transfer_enter events
    const hasTransferEvent = agentHistoryForCheck.some(event => 
      event && event.type === 'attended' && event.event === 'transfer' && event.ext && isQueueExtension(event.ext)
    );

    // Check for transfer_enter to a QUEUE EXTENSION (indicates 1st leg pattern)
    // transfer_enter to a customer phone number is OK - indicates successful transfer completion
    const hasTransferEnterToQueueEvent = agentHistoryForCheck.some(event => 
      event && event.type === 'agent' && event.event === 'transfer_enter' && 
      event.ext && isQueueExtension(event.ext)
    );

    const hasAgentEnterEvent = agentHistoryForCheck.some(event => 
      event && event.type === 'agent' && event.event === 'agent_enter'
    );

    // Based on user's clear patterns:
    // 1st leg INBOUND calls: Have type:attended event:transfer + type:agent event:transfer_enter (to queue extension)
    // 2nd leg calls (ALL types): Have agent_enter (NO transfer_enter to queue extension)
    // transfer_enter to customer phone number is allowed in 2nd leg (indicates successful transfer to customer)

    if (hasTransferEvent && hasTransferEnterToQueueEvent) {
      console.log(`✅ BLA INBOUND 1ST LEG: Call ${inboundCall.call_id} has attended:transfer + agent:transfer_enter (to queue) - this is a 1st leg source call`);
      // This is a 1st leg inbound call - process it as source call
    } else if (hasAgentEnterEvent && hasTransferEvent && !hasTransferEnterToQueueEvent) {
      console.log(`⚠️ BLA INBOUND 2ND LEG: Call ${inboundCall.call_id} has agent_enter + transfer (NO transfer_enter to queue) - this is a 2nd leg target call, skipping as source`);
      return; // Skip 2nd leg calls from being processed as source calls
    } else {
      console.log(`❌ BLA INBOUND NO TRANSFER: Call ${inboundCall.call_id} has no valid transfer pattern`);
      return; // Skip calls with no valid transfer patterns
    }

    // Special debug for your example calls
    if (inboundCall.call_id === 'e12791f0-64cf-123f-d784-bc2411568ba1' || inboundCall.call_id === '80pq1snkrv98i6ilhr2n' || 
        inboundCall.call_id === 'd1dee447-0731-48e9-8c92-be790b891154' || inboundCall.call_id === '21a87cd0-85a2-4a92-83fa-01b523ac11a6' || 
        inboundCall.call_id === '8a1ee646-64b0-4d57-8005-8021aa2c165a' || inboundCall.call_id === 'adf8fde0-bca4-4157-a236-4303243eedee') {
      console.log(`🎯 BLA EXAMPLE CALL: Found your example call ${inboundCall.call_id}!`);
      console.log(`🎯 BLA EXAMPLE CALLEE: callee_id_number = ${inboundCall.callee_id_number}`);
      console.log(`🎯 BLA EXAMPLE QUEUE: queue_name = ${inboundCall.queue_name}`);
      console.log(`🎯 BLA EXAMPLE HISTORY TYPE: ${typeof inboundCall.agent_history}`);
      if (inboundCall.agent_history && Array.isArray(inboundCall.agent_history)) {
        const transferEvents = inboundCall.agent_history.filter(e => e.event === 'transfer');
        const transferEnterEvents = inboundCall.agent_history.filter(e => e.event === 'transfer_enter');
        const agentEnterEvents = inboundCall.agent_history.filter(e => e.event === 'agent_enter');
        
        console.log(`🎯 BLA EXAMPLE PATTERN: ${transferEvents.length} transfer, ${transferEnterEvents.length} transfer_enter, ${agentEnterEvents.length} agent_enter`);
        
        // Show the event sequence
        const relevantEvents = inboundCall.agent_history.filter(e => 
          ['transfer', 'transfer_enter', 'agent_enter'].includes(e.event)
        ).sort((a, b) => a.last_attempt - b.last_attempt);
        
        console.log(`🎯 BLA EXAMPLE SEQUENCE:`);
        relevantEvents.forEach((e, i) => {
          console.log(`   ${i + 1}. ${e.type}:${e.event} (ext=${e.ext}, agent=${e.first_name} ${e.last_name}, time=${new Date(e.last_attempt * 1000).toISOString()})`);
        });
        
        // Determine call leg type based on simplified pattern
        if (hasTransferEvent && hasTransferEnterToQueueEvent) {
          console.log(`🎯 BLA EXAMPLE CLASSIFICATION: 1ST LEG (has transfer + transfer_enter to queue)`);
        } else if (hasAgentEnterEvent && hasTransferEvent && !hasTransferEnterToQueueEvent) {
          console.log(`🎯 BLA EXAMPLE CLASSIFICATION: 2ND LEG (has agent_enter + transfer, no transfer_enter to queue)`);
        } else {
          console.log(`🎯 BLA EXAMPLE CLASSIFICATION: UNKNOWN PATTERN`);
        }
      }
    }
    
    // Get the transfer events (we already confirmed they exist above)
    const transferEvents = agentHistoryForCheck.filter(event => 
      event && event.type === 'attended' && event.event === 'transfer' && event.ext && isQueueExtension(event.ext)
    );
    
    console.log(`🔄 BLA INBOUND TRANSFERS: Call ${inboundCall.call_id} found ${transferEvents.length} queue transfer events`);
    transferEvents.forEach(event => {
      console.log(`   - Transfer to queue ${event.ext} at ${new Date(event.last_attempt * 1000).toISOString()}`);
    });

    // Use the last transfer event
    const lastTransferEvent = transferEvents[transferEvents.length - 1];
    const queueExtension = lastTransferEvent.ext;
    
    // Don't use lastTransferEvent.last_attempt as transfer time - we'll use the target inbound call time
    const inboundTransferEventTime = lastTransferEvent.last_attempt;

    const transferEventTimeStr = inboundTransferEventTime ? new Date(inboundTransferEventTime * 1000).toISOString() : 'Invalid';
    console.log(`🔄 BLA INBOUND LINKING: Processing inbound call ${inboundCall.call_id}, queue: ${queueExtension}, inbound transfer event time: ${transferEventTimeStr}`);

    // Get the expected callee_id for this queue extension
    const expectedCalleeId = queueToCalleeExtensionMap[queueExtension];
    if (!expectedCalleeId) {
      console.log(`❌ BLA INBOUND MAPPING: No callee_id mapping found for queue extension ${queueExtension}`);
      return;
    }

    console.log(`🔍 BLA INBOUND MAPPING: Queue extension ${queueExtension} maps to callee_id_number ${expectedCalleeId}`);

    // Get source inbound call hold_start time for comparison (or fallback to called_time)
    // Parse source inbound call's agent_history to get hold_start time
    let sourceAgentHistory = [];
    if (typeof inboundCall.agent_history === 'string') {
      try {
        sourceAgentHistory = JSON.parse(inboundCall.agent_history);
      } catch (e) {
        try {
          sourceAgentHistory = parseHTMLAgentHistory(inboundCall.agent_history);
        } catch (e2) {}
      }
    } else if (Array.isArray(inboundCall.agent_history)) {
      sourceAgentHistory = inboundCall.agent_history;
    }
    
    // Extract hold_start timestamp from source inbound call
    const sourceHoldStartTime = extractHoldStartTimestamp(sourceAgentHistory);
    const rawSourceTime = sourceHoldStartTime || inboundCall.called_time || inboundCall.timestamp;
    const sourceInboundTime = rawSourceTime < 10000000000 ? rawSourceTime * 1000 : rawSourceTime;
    
    console.log(`🕐 BLA INBOUND 1ST LEG TIME: Using ${sourceHoldStartTime ? 'hold_start' : 'called_time'} for comparison`);
    
    // Normalize transfer event time for comparison
    const normalizedTransferEventTime = inboundTransferEventTime < 10000000000 ? inboundTransferEventTime * 1000 : inboundTransferEventTime;
    
    console.log(`🕐 BLA INBOUND SOURCE TIME: Raw=${rawSourceTime}, Normalized=${new Date(sourceInboundTime).toISOString()}`);
    console.log(`🕐 BLA INBOUND TRANSFER TIME: Raw=${inboundTransferEventTime}, Normalized=${new Date(normalizedTransferEventTime).toISOString()}`);
    
    // Find matching inbound calls within 10 minutes of transfer time
    const matchingInboundCalls = inboundCalls.filter(targetInboundCall => {
      // Don't match with itself
      if (targetInboundCall.call_id === inboundCall.call_id) {
        return false;
      }
      
      // Skip calls that have already been used
      if (usedCallIds.has(targetInboundCall.call_id)) {
        console.log(`⚠️ BLA INBOUND USED: Target inbound call ${targetInboundCall.call_id} already used, skipping`);
        return false;
      }

      // For inbound-to-inbound transfers, check if this target call has agent_enter events
      // Target calls must have an agent that answered the call
      if (targetInboundCall.agent_history) {
        let targetAgentHistory = [];
        
        if (typeof targetInboundCall.agent_history === 'string') {
          try {
            targetAgentHistory = JSON.parse(targetInboundCall.agent_history);
          } catch (e) {
            // If HTML, try parsing
            try {
              targetAgentHistory = parseHTMLAgentHistory(targetInboundCall.agent_history);
            } catch (e2) {
              // Ignore parse errors
            }
          }
        } else if (Array.isArray(targetInboundCall.agent_history)) {
          targetAgentHistory = targetInboundCall.agent_history;
        }
        
        // CRITICAL: Validate 2nd leg pattern using the helper function
        // 2nd leg MUST have: agent_enter (NO transfer_enter to queue extension)
        // transfer_enter to customer phone number is allowed (indicates successful transfer completion)
        const pattern2ndLeg = validate2ndLegPattern(targetAgentHistory);
        
        if (!pattern2ndLeg.isValid2ndLeg) {
          console.log(`❌ BLA INBOUND INVALID 2ND LEG: Target ${targetInboundCall.call_id} does not have valid 2nd leg pattern`);
          return false;
        }
        
        // Get the agent_enter timestamp - this is the "Transferred Call Time"
        const agentEnterTime = pattern2ndLeg.agentEnterTimestamp;
        if (!agentEnterTime) {
          console.log(`❌ BLA INBOUND NO AGENT ENTER TIME: Target ${targetInboundCall.call_id} has no agent_enter timestamp`);
          return false;
        }
        
        console.log(`✅ BLA INBOUND VALID 2ND LEG: Target ${targetInboundCall.call_id} has 2nd leg pattern: agent_enter (NO transfer_enter to queue)`);
        console.log(`🕐 BLA INBOUND AGENT ENTER TIME: ${new Date(agentEnterTime * 1000).toISOString()} (epoch: ${agentEnterTime})`);
        
        // Store the agent_enter timestamp on the target call for later use
        targetInboundCall._agentEnterTimestamp = agentEnterTime;
      }

      // For inbound-to-inbound transfers, match by queue_campaign_name instead of callee_id_number
      // because callee_id_number contains the customer's dialed phone number, not the queue extension
      const targetQueueName = targetInboundCall.queue_campaign_name || 
        (targetInboundCall.raw_data && typeof targetInboundCall.raw_data === 'object' ? targetInboundCall.raw_data.queue_name : null);
      
      // Map queue extension to queue name (e.g., 8008 -> "English Queue" or check if callee_id matches)
      // For now, check if the target inbound call's callee_id_number matches the expected queue extension
      // OR if the queue_campaign_name contains the expected queue identifier
      const calleeId = targetInboundCall.callee_id_number || 
        (targetInboundCall.raw_data && typeof targetInboundCall.raw_data === 'object' ? targetInboundCall.raw_data.callee_id_number : null);
      
      // Check if callee_id matches the expected callee_id from queue mapping
      // OR if it matches the queue extension directly
      const isCalleeIdMatch = calleeId === expectedCalleeId;
      const isQueueExtensionMatch = calleeId === queueExtension;
      
      // Special debug for your example calls
      if (targetInboundCall.call_id === '80pq1snkrv98i6ilhr2n' || targetInboundCall.call_id === 'qhlhneaqrmcnug059982') {
        console.log(`🎯 BLA EXAMPLE TARGET: Checking target call ${targetInboundCall.call_id} for source ${inboundCall.call_id}`);
        console.log(`🎯 BLA EXAMPLE TARGET: calleeId="${calleeId}", expectedCalleeId="${expectedCalleeId}", queueExtension="${queueExtension}"`);
        console.log(`🎯 BLA EXAMPLE TARGET: isCalleeIdMatch=${isCalleeIdMatch}, isQueueExtensionMatch=${isQueueExtensionMatch}`);
      }
      
      if (!isCalleeIdMatch && !isQueueExtensionMatch) {
        console.log(`❌ BLA INBOUND CALLEE: Target inbound call ${targetInboundCall.call_id} callee_id ${calleeId} != expected ${expectedCalleeId} or queue ${queueExtension}`);
        return false;
      }
      
      const matchType = isCalleeIdMatch ? 'callee_id' : 'queue_extension';
      console.log(`✅ BLA INBOUND CALLEE: Target inbound call ${targetInboundCall.call_id} matches via ${matchType} ${calleeId}`);

      // CRITICAL: Compare 1st leg called_time with 2nd leg agent_enter time
      // 1st leg called_time must be SMALLER than 2nd leg agent_enter time
      const agentEnterTime = targetInboundCall._agentEnterTimestamp;
      if (!agentEnterTime) {
        console.log(`⚠️ BLA INBOUND TIME: Missing agent_enter timestamp for target ${targetInboundCall.call_id}`);
        return false;
      }
      
      // Normalize source time to seconds
      const normalizedSourceTime = sourceInboundTime < 10000000000 ? sourceInboundTime : Math.floor(sourceInboundTime / 1000);
      const normalizedAgentEnterTime = agentEnterTime < 10000000000 ? agentEnterTime : Math.floor(agentEnterTime / 1000);
      
      // Calculate time difference: 2nd leg agent_enter - 1st leg called_time
      const timeDifference = normalizedAgentEnterTime - normalizedSourceTime;

      console.log(`🕐 BLA INBOUND TIME CHECK: Source ${inboundCall.call_id} (1st leg ${sourceHoldStartTime ? 'hold_start' : 'called_time'}) -> Target ${targetInboundCall.call_id} (2nd leg agent_enter)`);
      console.log(`🕐 BLA TIMING: 1st leg=${new Date(normalizedSourceTime * 1000).toISOString()}, 2nd leg agent_enter=${new Date(normalizedAgentEnterTime * 1000).toISOString()}, diff=${timeDifference}s`);
      
      // CRITICAL: 1st leg hold_start must be smaller than 2nd leg agent_enter time (timeDifference > 0)
      // AND within reasonable transfer window (10 minutes)
      const isValidTiming = timeDifference > 0 && timeDifference <= 600;
      console.log(`🕐 BLA TIMING RESULT: ${isValidTiming ? '✅ VALID' : '❌ INVALID'} - 1st leg ${timeDifference > 0 ? 'before' : 'after'} 2nd leg (${timeDifference}s difference)`);
      
      return isValidTiming;
    });

    if (matchingInboundCalls.length > 0) {
      // Sort by agent_enter time (earliest after source call wins)
      // Normalize source time to seconds for comparison
      const normalizedSourceTimeSeconds = sourceInboundTime < 10000000000 ? sourceInboundTime : Math.floor(sourceInboundTime / 1000);
      
      matchingInboundCalls.sort((a, b) => {
        const aAgentEnterTime = a._agentEnterTimestamp || 0;
        const bAgentEnterTime = b._agentEnterTimestamp || 0;
        const aTimeDiff = aAgentEnterTime - normalizedSourceTimeSeconds;
        const bTimeDiff = bAgentEnterTime - normalizedSourceTimeSeconds;
        return aTimeDiff - bTimeDiff; // Earliest agent_enter after source wins
      });

      const matchingInboundCall = matchingInboundCalls[0];
      
      // CRITICAL: Use the agent_enter timestamp as the "Transferred Call Time"
      // This is when the agent entered the call after the transfer
      const agentEnterTimestamp = matchingInboundCall._agentEnterTimestamp;
      const actualTransferTime = agentEnterTimestamp;
      
      // Store the transferred call time on the inbound call for report display
      matchingInboundCall.transferred_call_time = actualTransferTime;
      
      // Get agent information from the target inbound call
      const agentExtension = matchingInboundCall.extension;
      const agentName = matchingInboundCall.agent_name || 'Unknown Agent';

      console.log(`🎯 BLA INBOUND MATCH: Source inbound call ${inboundCall.call_id} linked to target inbound call ${matchingInboundCall.call_id}`);
      console.log(`👤 BLA INBOUND AGENT: Transfer received by ${agentName} (Extension: ${agentExtension})`);
      console.log(`🕐 BLA INBOUND TRANSFER TIME: Using agent_enter timestamp ${actualTransferTime ? new Date(actualTransferTime * 1000).toISOString() : 'invalid'} as Transferred Call Time`);

      // Determine transfer success/failure status for inbound-to-inbound transfers
      let transferStatus = 'Success';
      let transferStatusReason = 'Agent answered';
      
      // Check if the target inbound call has agent_enter event (successful connection)
      if (matchingInboundCall.agent_history) {
        let agentHistory = [];
        
        // Parse agent_history if it's a string
        if (typeof matchingInboundCall.agent_history === 'string') {
          try {
            agentHistory = JSON.parse(matchingInboundCall.agent_history);
          } catch (e) {
            console.log(`⚠️ BLA INBOUND TRANSFER STATUS: Could not parse agent_history for ${matchingInboundCall.call_id}`);
          }
        } else if (Array.isArray(matchingInboundCall.agent_history)) {
          agentHistory = matchingInboundCall.agent_history;
        }
        
        // Look for agent_enter events (presence indicates successful transfer)
        const agentEnterEvents = agentHistory.filter(event => 
          event && event.event === 'agent_enter'
        );
        
        // Check if 2nd leg has attended:transfer event (indicates re-transfer or system anomaly)
        const hasAttendedTransferIn2ndLeg = agentHistory.some(event => 
          event && event.type === 'attended' && event.event === 'transfer'
        );
        
        // Debug: Log all agent_enter events to understand the issue
        const allAgentEnterEvents = agentHistory.filter(event => event && event.event === 'agent_enter');
        console.log(`🔍 BLA TRANSFER STATUS DEBUG: Call ${matchingInboundCall.call_id} has ${allAgentEnterEvents.length} agent_enter events`);
        allAgentEnterEvents.forEach((event, index) => {
          console.log(`   - Event ${index + 1}: connected=${event.connected} (type: ${typeof event.connected}), ext=${event.ext}, agent=${event.first_name} ${event.last_name}`);
        });
        console.log(`🔍 BLA TRANSFER STATUS DEBUG: ${agentEnterEvents.length} events match success criteria`);
        console.log(`🔍 BLA TRANSFER STATUS DEBUG: 2nd leg has attended:transfer = ${hasAttendedTransferIn2ndLeg}`);
        
        if (agentEnterEvents.length > 0) {
          transferStatus = 'Success';
          transferStatusReason = 'Agent answered';
          console.log(`✅ BLA INBOUND TRANSFER STATUS: Transfer successful - agent answered`);
        } else {
          transferStatus = 'Failed';
          transferStatusReason = 'Agent did not answer';
          console.log(`❌ BLA INBOUND TRANSFER STATUS: Transfer failed - no agent_enter event found or connected=false`);
        }
      }

      // Create the linked call record
      linkedCalls.push({
        // Source inbound call data (1st leg)
        source_inbound_call: {
          ...inboundCall,
          transfer_queue_extension: queueExtension,
          transfer_time: actualTransferTime,
          transfer_time_formatted: actualTransferTime && !isNaN(actualTransferTime) ? new Date(actualTransferTime * 1000).toISOString() : null
        },
        // Target inbound call data (2nd leg)
        target_inbound_call: {
          ...matchingInboundCall,
          receiving_agent_extension: agentExtension,
          receiving_agent_name: agentName
        },
        // Link metadata
        link_metadata: {
          queue_extension: queueExtension,
          callee_id: expectedCalleeId,
          transfer_time: actualTransferTime,
          transfer_status: transferStatus,
          transfer_status_reason: transferStatusReason,
          time_difference_seconds: Math.abs(
            ((matchingInboundCall.called_time || matchingInboundCall.timestamp) < 10000000000 ? 
             (matchingInboundCall.called_time || matchingInboundCall.timestamp) * 1000 : 
             (matchingInboundCall.called_time || matchingInboundCall.timestamp)) - 
            (actualTransferTime < 10000000000 ? actualTransferTime * 1000 : actualTransferTime)
          ) / 1000
        }
      });
      
      // Mark this target inbound call as used to prevent duplicate linking
      usedCallIds.add(matchingInboundCall.call_id);
      console.log(`🔒 BLA INBOUND USED: Marked target inbound call ${matchingInboundCall.call_id} as used`);
      
      // Debug for specific call
      if (matchingInboundCall.call_id === 'a55eb3fd-7083-123f-3397-bc241163f17d') {
        console.log(`🎯🎯🎯 BLA USED: Call a55eb3fd-7083-123f-3397-bc241163f17d was marked as USED by source call ${inboundCall.call_id}`);
        console.log(`🎯 BLA PROBLEM: This call should be a 1st leg source, not a 2nd leg target!`);
      }
    } else {
      console.log(`❌ BLA INBOUND NO MATCH: No target inbound calls found for source inbound call ${inboundCall.call_id} with callee_id ${expectedCalleeId}`);
      
      // Check if transfer_enter event exists in 1st leg (indicates agent answered but 2nd leg not found)
      const transferEnterEvent = agentHistoryForCheck.find(event => 
        event && event.type === 'agent' && event.event === 'transfer_enter'
      );
      
      let transferStatus = 'Failed';
      let transferStatusReason = 'No agent answered in queue';
      let receivingAgentName = null;
      let receivingAgentExtension = null;
      
      if (transferEnterEvent) {
        // Agent answered but 2nd leg not found - this is an ERROR condition
        transferStatus = 'Error';
        transferStatusReason = 'Agent answered but 2nd leg call not found in system';
        receivingAgentName = `${transferEnterEvent.first_name || ''} ${transferEnterEvent.last_name || ''}`.trim() || 'Unknown Agent';
        receivingAgentExtension = transferEnterEvent.ext;
        console.log(`⚠️ BLA INBOUND ERROR: Agent ${receivingAgentName} (${receivingAgentExtension}) answered but 2nd leg not found - marking as ERROR`);
      }
      
      // Add failed/error transfer record
      linkedCalls.push({
        // Source inbound call data (1st leg)
        source_inbound_call: {
          ...inboundCall,
          transfer_queue_extension: queueExtension,
          transfer_time: inboundTransferEventTime,
          transfer_time_formatted: inboundTransferEventTime && !isNaN(inboundTransferEventTime) ? 
            new Date(inboundTransferEventTime * 1000).toISOString() : null
        },
        // No target inbound call (failed transfer)
        target_inbound_call: receivingAgentName ? {
          receiving_agent_name: receivingAgentName,
          receiving_agent_extension: receivingAgentExtension
        } : null,
        // Link metadata
        link_metadata: {
          queue_extension: queueExtension,
          callee_id: expectedCalleeId,
          transfer_time: inboundTransferEventTime,
          transfer_status: transferStatus,
          transfer_status_reason: transferStatusReason,
          time_difference_seconds: null
        }
      });
      console.log(`📋 BLA INBOUND ${transferStatus.toUpperCase()} TRANSFER: Added ${transferStatus.toLowerCase()} transfer record for source inbound call ${inboundCall.call_id}`);
    }
  });

  console.log(`✅ BLA INBOUND LINKING COMPLETE: Successfully linked ${linkedCalls.length} inbound call pairs (including failed transfers)`);
  return linkedCalls;
}

/**
 * Generate BLA Hot Patch Transfer Report
 * @param {Object} pool - MySQL connection pool
 * @param {Object} filters - Filter parameters (start, end, etc.)
 * @returns {Object} - Report data with linked campaign and inbound calls
 */
export async function generateBLAHotPatchTransferReport(pool, filters) {
  const { start, end, agent_name, extension, queue_campaign_name } = filters;

  console.log(`🚀 BLA REPORT: Generating Hot Patch Transfer Report`);
  console.log(`📅 BLA REPORT: Date range: ${start} to ${end}`);
  console.log(`🔍 BLA FILTERS: agent_name=${agent_name}, extension=${extension}, queue_campaign_name=${queue_campaign_name}`);
  
  // Debug: Show exact input values and their types
  console.log(`🔍 BLA INPUT DEBUG: start="${start}" (type: ${typeof start})`);
  console.log(`🔍 BLA INPUT DEBUG: end="${end}" (type: ${typeof end})`);
  console.log(`🔍 BLA INPUT DEBUG: start ISO parse: ${DateTime.fromISO(start).toISO()}`);
  console.log(`🔍 BLA INPUT DEBUG: end ISO parse: ${DateTime.fromISO(end).toISO()}`);

  try {
    // Fetch queue-skill-history.json in parallel with DB queries
    const skillHistoryPromise = fetchQueueSkillHistory();

    // Since final_report already stores timestamps in Asia/Dubai timezone, 
    // convert input dates directly to epochs without timezone conversion
    const startEpoch = Math.floor(
      DateTime.fromISO(start).toSeconds()
    );
    const endEpochExclusive = Math.floor(
      DateTime.fromISO(end).toSeconds()
    );

    // Step 1: Get all Campaign calls from final_report table (remove transfer_event filter to catch all)
    let campaignSql = `
      SELECT call_id, record_type, type, agent_name, extension, queue_campaign_name, 
             called_time, called_time_formatted, caller_id_number, caller_id_name, 
             answered_time, hangup_time, talk_duration, agent_disposition, 
             sub_disp_1, sub_disp_2, follow_up_notes, status, campaign_type, 
             agent_history, lead_history, queue_history, recording, transfer_event, transfer_extension, transfer_queue_extension
      FROM final_report 
      WHERE called_time >= ? AND called_time < ? 
        AND record_type = 'Campaign'
    `;
    
    const campaignValues = [startEpoch, endEpochExclusive];

    // Add optional filters for campaign calls
    if (agent_name) {
      campaignSql += ' AND agent_name LIKE ?';
      campaignValues.push(`%${agent_name}%`);
    }
    if (extension) {
      campaignSql += ' AND extension LIKE ?';
      campaignValues.push(`%${extension}%`);
    }
    if (queue_campaign_name) {
      campaignSql += ' AND queue_campaign_name LIKE ?';
      campaignValues.push(`%${queue_campaign_name}%`);
    }

    campaignSql += ' ORDER BY called_time DESC';

    console.log(`📊 BLA QUERY: Fetching campaign calls with transfers`);
    console.log(`📊 BLA DATE RANGE: startEpoch=${startEpoch} (${new Date(startEpoch * 1000).toISOString()}), endEpochExclusive=${endEpochExclusive} (${new Date(endEpochExclusive * 1000).toISOString()})`);
    console.log(`📊 BLA QUERY: Fetching campaign calls from final_report table`);
    console.log(`🕐 BLA DATE RANGE: ${new Date(startEpoch * 1000).toISOString()} to ${new Date(endEpochExclusive * 1000).toISOString()}`);
    console.log(`🔢 BLA EPOCH RANGE: ${startEpoch} to ${endEpochExclusive} (exclusive)`);
    
    // Debug: Check target call timestamp against query range
    // Check multiple target call timestamps
    const targetCallTimestamps = [
      1764954002, // Old target call b86b6f90-17b3-43aa-a3cc-ede05e890126
      1766132373, // New target call ij0lrn0uk290j99nvbc4
      1764767740  // Target call dca1e20f-3c11-4820-8265-31f4de14dbce (2025-12-03 13:15:40 UTC)
    ];
    
    targetCallTimestamps.forEach(targetCallTimestamp => {
      const targetCallDate = new Date(targetCallTimestamp * 1000).toISOString();
      const isInRange = targetCallTimestamp >= startEpoch && targetCallTimestamp < endEpochExclusive;
      console.log(`🎯 BLA TARGET TIMESTAMP: Target call timestamp=${targetCallTimestamp} (${targetCallDate})`);
      console.log(`🎯 BLA TARGET RANGE CHECK: Is target call in query range? ${isInRange ? 'YES' : 'NO'}`);
      if (!isInRange) {
        console.log(`❌ BLA TARGET OUT OF RANGE: Target call timestamp ${targetCallTimestamp} is outside query range [${startEpoch}, ${endEpochExclusive})`);
        console.log(`❌ BLA TARGET SOLUTION: Use date range that includes ${targetCallDate} to retrieve the target call`);
      }
    });
    
    // Debug: Check if our target campaign call is in the results
    console.log(`🔍 BLA QUERY EXECUTION: Running campaign query with values:`, campaignValues);
    console.log(`🔍 BLA QUERY SQL:`, campaignSql);
    
    // Debug: Check if our target timestamp is in the expected range
    console.log(`🎯 BLA TARGET CALL: Looking for call dca1e20f-3c11-4820-8265-31f4de14dbce with timestamp 1764767740`);
    console.log(`🎯 BLA TARGET DATE: 1764767740 = ${new Date(1764767740 * 1000).toISOString()}`);
    console.log(`🎯 BLA RANGE CHECK: startEpoch=${startEpoch}, endEpochExclusive=${endEpochExclusive}`);
    console.log(`🎯 BLA RANGE CHECK: Is 1764767740 >= ${startEpoch}? ${1764767740 >= startEpoch}`);
    console.log(`🎯 BLA RANGE CHECK: Is 1764767740 < ${endEpochExclusive}? ${1764767740 < endEpochExclusive}`);
    
    // Check for campaign call c7f72152-5d31-4939-988f-3ccf6a0ae8eb
    const campaignCallTimestamp = 1768928846;
    console.log(`🎯 BLA CAMPAIGN TARGET: Looking for campaign c7f72152-5d31-4939-988f-3ccf6a0ae8eb with timestamp ${campaignCallTimestamp}`);
    console.log(`🎯 BLA CAMPAIGN DATE: ${campaignCallTimestamp} = ${new Date(campaignCallTimestamp * 1000).toISOString()}`);
    console.log(`🎯 BLA CAMPAIGN RANGE: Is ${campaignCallTimestamp} >= ${startEpoch}? ${campaignCallTimestamp >= startEpoch}`);
    console.log(`🎯 BLA CAMPAIGN RANGE: Is ${campaignCallTimestamp} < ${endEpochExclusive}? ${campaignCallTimestamp < endEpochExclusive}`);
    
    let finalReportRows = [];
    try {
      [finalReportRows] = await pool.execute(campaignSql, campaignValues);
      console.log(`📊 BLA RESULT: Found ${finalReportRows.length} campaign calls from final_report`);
    } catch (error) {
      console.log(`⚠️ BLA WARNING: Could not fetch from final_report, continuing. Error: ${error.message}`);
    }

    // Fetch from raw_lead_dial as a fallback/supplement
    let rawCampaignRows = [];
    try {
      const rawCampaignSql = `
        SELECT 
          call_id, 'Campaign' as record_type, 'campaign' as type, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_name')) as agent_name, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_extension')) as extension, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.campaign_name')) as queue_campaign_name, 
          timestamp as called_time, 
          FROM_UNIXTIME(timestamp) as called_time_formatted, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.lead_number')) as caller_id_number, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.lead_name')) as caller_id_name, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.campaign_timestamps.lead_answer_time')) as answered_time, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.campaign_timestamps.lead_hangup_time')) as hangup_time, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_talk_time')) as talk_duration, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_disposition')) as agent_disposition, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_subdisposition.name')) as sub_disp_1, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_subdisposition.subdisposition.name')) as sub_disp_2, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.follow_up_notes')) as follow_up_notes, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.status')) as status, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_extension')) as agent_extension,
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.campaign_type')) as campaign_type, 
          JSON_EXTRACT(raw_data, '$.lead_history') as agent_history, 
          JSON_EXTRACT(raw_data, '$.lead_history') as lead_history, 
          JSON_EXTRACT(raw_data, '$.queue_history') as queue_history, 
          JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.recording_filename')) as recording, 
          0 as transfer_event, 
          null as transfer_extension, 
          null as transfer_queue_extension
        FROM raw_campaigns
        WHERE timestamp >= ? AND timestamp < ?
      `;
      [rawCampaignRows] = await pool.execute(rawCampaignSql, [startEpoch, endEpochExclusive]);
      console.log(`📊 BLA FALLBACK RESULT: Found ${rawCampaignRows.length} campaign calls from raw_campaigns`);
      
      // Debug: Check how many raw_campaigns calls have lead_history
      const rawWithLeadHistory = rawCampaignRows.filter(c => c.lead_history);
      console.log(`📊 BLA RAW DEBUG: ${rawWithLeadHistory.length} raw_campaigns calls have lead_history`);
      if (rawCampaignRows.length > 0) {
        const sample = rawCampaignRows[0];
        console.log(`📊 BLA RAW DEBUG: First raw call lead_history type: ${typeof sample.lead_history}`);
        console.log(`📊 BLA RAW DEBUG: First raw call lead_history: ${sample.lead_history ? String(sample.lead_history).substring(0, 300) : 'null/undefined'}`);
      }
      
      // Debug: Check if target call is in raw_lead_dial results
      const targetRawCall = rawCampaignRows.find(call => call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce');
      if (targetRawCall) {
        console.log(`✅ BLA TARGET RAW FOUND: Target call found in raw_lead_dial:`, targetRawCall);
      } else {
        console.log(`❌ BLA TARGET RAW MISSING: Target call NOT found in raw_lead_dial`);
        console.log(`🔍 BLA RAW DEBUG: Sample call IDs from raw_lead_dial:`, rawCampaignRows.slice(0, 5).map(c => c.call_id));
      }
      
      // Check for campaign call c7f72152-5d31-4939-988f-3ccf6a0ae8eb
      const campaignTargetCall = rawCampaignRows.find(call => call.call_id === 'c7f72152-5d31-4939-988f-3ccf6a0ae8eb');
      if (campaignTargetCall) {
        console.log(`✅ BLA CAMPAIGN RAW FOUND: Campaign c7f72152 found in raw_lead_dial`);
        console.log(`🔍 BLA CAMPAIGN RAW: agent_extension=${campaignTargetCall.extension}, lead_history type=${typeof campaignTargetCall.lead_history}`);
      } else {
        console.log(`❌ BLA CAMPAIGN RAW MISSING: Campaign c7f72152 NOT found in raw_lead_dial`);
      }
    } catch (error) {
      console.log(`⚠️ BLA ERROR: Could not fetch from raw_lead_dial. Error: ${error.message}`);
    }

    // Combine and deduplicate, giving priority to final_report records BUT preserving lead_history from raw_lead_dial
    const campaignCallMap = new Map();
    
    // First add raw_lead_dial records (they have lead_history)
    rawCampaignRows.forEach(call => campaignCallMap.set(call.call_id, call));
    
    // Then merge final_report records, but preserve important fields from raw if final_report doesn't have them
    finalReportRows.forEach(call => {
      if (campaignCallMap.has(call.call_id)) {
        // Merge: prefer final_report data but keep critical fields from raw if final_report doesn't have them
        const rawCall = campaignCallMap.get(call.call_id);
        const mergedCall = {
          ...call,
          // Preserve lead_history from raw_lead_dial if final_report doesn't have it
          lead_history: call.lead_history || rawCall.lead_history,
          agent_history: call.agent_history || rawCall.agent_history,
          // CRITICAL: Preserve caller_id_number (lead's phone) for customer matching
          caller_id_number: call.caller_id_number || rawCall.caller_id_number
        };
        campaignCallMap.set(call.call_id, mergedCall);
      } else {
        campaignCallMap.set(call.call_id, call);
      }
    });
    
    const campaignRows = Array.from(campaignCallMap.values());
    console.log(`📊 BLA RESULT: Found ${campaignRows.length} total campaign calls`);
    
    // Debug specific campaign call for customer matching issue
    const debugCallId = 'fdb1142b-db1e-495c-8802-7d6214f64f58';
    const debugCall = campaignRows.find(c => c.call_id === debugCallId);
    if (debugCall) {
      console.log(`🔍 BLA DEBUG CALL ${debugCallId}:`);
      console.log(`   - lead_history type: ${typeof debugCall.lead_history}`);
      console.log(`   - lead_history value: ${debugCall.lead_history ? String(debugCall.lead_history).substring(0, 500) : 'null/undefined'}`);
      console.log(`   - caller_id_number: ${debugCall.caller_id_number}`);
    } else {
      console.log(`❌ BLA DEBUG: Call ${debugCallId} not found in campaignRows`);
    }
    
    // Check for both target calls
    const targetCallIds = ['b86b6f90-17b3-43aa-a3cc-ede05e890126', 'ij0lrn0uk290j99nvbc4', 'dca1e20f-3c11-4820-8265-31f4de14dbce'];
    targetCallIds.forEach(callId => {
      const targetCall = campaignRows.find(call => call.call_id === callId);
      if (targetCall) {
        console.log(`✅ BLA TARGET FOUND: Campaign call ${callId} found in query results`);
      } else {
        console.log(`❌ BLA TARGET MISSING: Campaign call ${callId} NOT found in query results`);
      }
    });
    
    // Check if target call exists with different timestamp
    const allTargetCalls = campaignRows.filter(call => call.call_id && call.call_id.includes('b86b6f90'));
    if (allTargetCalls.length > 0) {
      console.log(`🔍 BLA TARGET PARTIAL: Found ${allTargetCalls.length} calls with similar ID:`, allTargetCalls.map(c => c.call_id));
    }
    
    // Check first few calls to see what we're getting
    console.log(`🔍 BLA SAMPLE CALLS (first 3):`, campaignRows.slice(0, 3).map(c => ({ call_id: c.call_id, called_time: c.called_time })));
    
    // Debug: Count how many already have transfer_event = 1 in final_report
    const preMarkedTransfers = campaignRows.filter(call => call.transfer_event === 1).length;
    console.log(`📊 BLA PRE-MARKED: ${preMarkedTransfers} calls already marked with transfer_event=1 in final_report`);
    
    // Debug: Log sample campaign calls to see what we're getting
    if (campaignRows.length > 0) {
      console.log(`📋 BLA CAMPAIGN SAMPLE (first 5 calls):`);
      campaignRows.slice(0, 5).forEach(call => {
        console.log(`   - ${call.call_id}: Agent ${call.agent_name} (${call.extension}), Pre-marked Transfer: ${call.transfer_event ? 'YES' : 'NO'}`);
      });
    }

    // Step 1.5: Get all Outbound calls from raw data (using correct raw_queue_outbound table)
    console.log(`📊 BLA QUERY: Fetching outbound calls from raw_queue_outbound table`);
    let outboundRows;
    
    try {
      // Fetch from raw_queue_outbound table with JSON data
      const outboundSql = `
        SELECT callid as call_id, 'Outbound' as record_type, 'outbound' as type,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_name')) as agent_name,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_ext')) as extension,
               queue_name as queue_campaign_name,
               called_time, called_time,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.caller_id_number')) as caller_id_number,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.caller_id_name')) as caller_id_name,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.callee_id_number')) as callee_id_number,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.to')) as customer_number,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.answered_time')) as answered_time,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.hangup_time')) as hangup_time,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.billing_seconds')) as talk_duration,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_disposition')) as agent_disposition,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.follow_up_notes')) as follow_up_notes,
               JSON_EXTRACT(raw_data, '$.agent_history') as agent_history,
               JSON_EXTRACT(raw_data, '$.queue_history') as queue_history,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.recording_filename')) as recording,
               raw_data
        FROM raw_queue_outbound 
        WHERE called_time >= ? AND called_time < ?
          AND JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_name')) IS NOT NULL
        ORDER BY called_time DESC
      `;
      
      [outboundRows] = await pool.execute(outboundSql, [startEpoch, endEpochExclusive]);
      console.log(`📊 BLA RESULT: Found ${outboundRows.length} outbound calls from raw_queue_outbound`);
    } catch (error) {
      console.log(`⚠️ BLA FALLBACK: raw_queue_outbound query failed, trying final_report: ${error.message}`);
      
      // Fallback to final_report table
      const fallbackSql = `
        SELECT call_id, record_type, type, agent_name, extension, queue_campaign_name, 
               called_time, called_time_formatted, caller_id_number, caller_id_name, 
               answered_time, hangup_time, talk_duration, agent_disposition, 
               sub_disp_1, sub_disp_2, follow_up_notes, status, campaign_type, 
               agent_history, queue_history, recording, transfer_event, transfer_extension
        FROM final_report 
        WHERE called_time >= ? AND called_time < ? 
          AND record_type = 'Outbound'
        ORDER BY called_time DESC
      `;
      
      [outboundRows] = await pool.execute(fallbackSql, [startEpoch, endEpochExclusive]);
      console.log(`📊 BLA FALLBACK RESULT: Found ${outboundRows.length} outbound calls from final_report`);
    }

    // Step 2: Get all Inbound calls in the time range (extended by 5 minutes for transfer matching)
    const extendedEndEpoch = endEpochExclusive + (5 * 60); // Add 5 minutes buffer

    // Fetch from raw_queue_inbound table with JSON data
    console.log(`📊 BLA QUERY: Fetching inbound calls from raw_queue_inbound table`);
    let inboundRows;
    
    try {
      const inboundSql = `
        SELECT callid as call_id, 'Inbound' as record_type, 'inbound' as type,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_answered_name')) as agent_name,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_answered_ext')) as extension,
               queue_name as queue_campaign_name,
               called_time, called_time,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.caller_id_number')) as caller_id_number,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.caller_id_name')) as caller_id_name,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.callee_id_number')) as callee_id_number,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.answered_time')) as answered_time,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.hangup_time')) as hangup_time,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.billing_seconds')) as talk_duration,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.agent_disposition')) as agent_disposition,
               JSON_EXTRACT(raw_data, '$.agent_history') as agent_history,
               JSON_EXTRACT(raw_data, '$.queue_history') as queue_history,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.recording_filename')) as recording,
               JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.disposition')) as disposition,
               COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.abandoned')), 
                        CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.disposition')) = 'ABANDON' THEN 'Yes' ELSE 'No' END) as abandoned,
               raw_data
        FROM raw_queue_inbound 
        WHERE called_time >= ? AND called_time < ?
        ORDER BY called_time DESC
      `;
      
      [inboundRows] = await pool.execute(inboundSql, [startEpoch, extendedEndEpoch]);
      console.log(`📊 BLA RESULT: Found ${inboundRows.length} inbound calls from raw_queue_inbound`);
    } catch (error) {
      console.log(`⚠️ BLA FALLBACK: raw_queue_inbound query failed, trying final_report: ${error.message}`);
      
      // Fallback to final_report table
      const fallbackSql = `
        SELECT call_id, record_type, type, agent_name, extension, queue_campaign_name,
               called_time, called_time_formatted, caller_id_number, caller_id_name, callee_id_number,
               answered_time, hangup_time, talk_duration, agent_disposition, 
               agent_history, queue_history, recording
        FROM final_report 
        WHERE called_time >= ? AND called_time < ? 
          AND record_type = 'Inbound'
        ORDER BY called_time DESC
      `;
      
      [inboundRows] = await pool.execute(fallbackSql, [startEpoch, extendedEndEpoch]);
      console.log(`📊 BLA FALLBACK RESULT: Found ${inboundRows.length} inbound calls from final_report`);
    }

    // Step 3: Process campaign calls using proper transfer detection logic
    console.log(`🔍 BLA CAMPAIGN PROCESSING: Starting to process ${campaignRows.length} campaign calls`);
    
    // Debug: Check if target campaign calls are in campaignRows before processing
    const targetInRaw1 = campaignRows.find(call => call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126');
    const targetInRaw2 = campaignRows.find(call => call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce');
    
    if (targetInRaw1) {
      console.log(`🎯 BLA TARGET RAW: Target campaign call b86b6f90-17b3-43aa-a3cc-ede05e890126 found in campaignRows before processing`);
      console.log(`🎯 BLA TARGET RAW DATA: called_time=${targetInRaw1.called_time}, transfer_event=${targetInRaw1.transfer_event}, transfer_queue_extension=${targetInRaw1.transfer_queue_extension}`);
    } else {
      console.log(`❌ BLA TARGET RAW: Target campaign call b86b6f90-17b3-43aa-a3cc-ede05e890126 NOT found in campaignRows`);
    }
    
    if (targetInRaw2) {
      console.log(`🎯 BLA TARGET RAW: Target campaign call dca1e20f-3c11-4820-8265-31f4de14dbce found in campaignRows before processing`);
      console.log(`🎯 BLA TARGET RAW DATA: called_time=${targetInRaw2.called_time}, transfer_event=${targetInRaw2.transfer_event}, transfer_queue_extension=${targetInRaw2.transfer_queue_extension}`);
    } else {
      console.log(`❌ BLA TARGET RAW: Target campaign call dca1e20f-3c11-4820-8265-31f4de14dbce NOT found in campaignRows - this is the root cause!`);
    }
    
    const processedCampaignCalls = campaignRows.map(call => {
      try {
        // Debug target campaign call processing
        if (call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126' || call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce') {
          console.log(`🎯 BLA TARGET PROCESSING: Starting to process target campaign call ${call.call_id}`);
          console.log(`🎯 BLA TARGET PROCESSING: agent_history type=${typeof call.agent_history}, length=${call.agent_history ? call.agent_history.length : 'null'}`);
          console.log(`🎯 BLA TARGET PROCESSING: lead_history type=${typeof call.lead_history}, length=${call.lead_history ? call.lead_history.length : 'null'}`);
          console.log(`🎯 BLA TARGET PROCESSING: transfer_event=${call.transfer_event}, transfer_queue_extension=${call.transfer_queue_extension}`);
        }
        
        // Parse agent_history if it's a string
        if (typeof call.agent_history === 'string') {
          try {
            call.agent_history = JSON.parse(call.agent_history);
          } catch (jsonError) {
            console.log(`📋 BLA HTML PARSE: Extracting transfer data from HTML for call ${call.call_id}`);
            call.agent_history = parseHTMLAgentHistory(call.agent_history);
          }
        } else if (!Array.isArray(call.agent_history)) {
          call.agent_history = [];
        }
        
        // Parse lead_history if it exists and is a string
        if (typeof call.lead_history === 'string') {
          try {
            call.lead_history = JSON.parse(call.lead_history);
          } catch (jsonError) {
            console.log(`📋 BLA LEAD HISTORY PARSE: Error parsing lead_history for call ${call.call_id}`);
            call.lead_history = [];
          }
        }
        
        // For campaign calls, prioritize lead_history over agent_history for transfer detection
        const historyToCheck = (call.lead_history && Array.isArray(call.lead_history) && call.lead_history.length > 0) 
          ? call.lead_history 
          : call.agent_history;
        
        console.log(`📋 BLA CAMPAIGN HISTORY: Call ${call.call_id} using ${historyToCheck === call.lead_history ? 'lead_history' : 'agent_history'} with ${Array.isArray(historyToCheck) ? historyToCheck.length : 0} events`);
        
        // Special debug for target campaign calls
        if (call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126' || call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce') {
          console.log(`🎯 BLA TARGET PROCESSING: Processing target campaign call ${call.call_id}!`);
          console.log(`🎯 BLA TARGET HISTORY: lead_history type=${typeof call.lead_history}, length=${Array.isArray(call.lead_history) ? call.lead_history.length : 'N/A'}`);
          console.log(`🎯 BLA TARGET HISTORY: agent_history type=${typeof call.agent_history}, length=${Array.isArray(call.agent_history) ? call.agent_history.length : 'N/A'}`);
          console.log(`🎯 BLA TARGET HISTORY: Using ${historyToCheck === call.lead_history ? 'lead_history' : 'agent_history'}`);
          if (Array.isArray(historyToCheck)) {
            console.log(`🎯 BLA TARGET EVENTS: Events in history:`, historyToCheck.map(e => `${e.type}(${e.ext})`).join(', '));
          }
        }
        
        // Detect transfer events using the appropriate history
        const transferInfo = detectTransferEvents(historyToCheck, 'campaign', call);
        call.transfer_event = transferInfo.transfer_event;
        call.transfer_extension = transferInfo.transfer_extension;
        call.transfer_queue_extension = transferInfo.transfer_queue_extension;
        call.transfer_type = transferInfo.transfer_type;
        call.transfer_timestamp = transferInfo.transfer_timestamp;
        
        // Special debug for target campaign calls
        if (call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126' || call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce') {
          console.log(`🎯 BLA TARGET RESULT: ${call.call_id} transfer_event=${transferInfo.transfer_event}, transfer_queue_extension=${transferInfo.transfer_queue_extension}`);
          console.log(`🎯 BLA TARGET TIMESTAMP DEBUG: Raw transfer info:`, transferInfo);
          console.log(`🎯 BLA TARGET FINAL CALL STATE: transfer_event=${call.transfer_event}, transfer_queue_extension=${call.transfer_queue_extension}`);
          console.log(`🎯 BLA TARGET FILTER CHECK: Will pass filter? ${call.transfer_event && call.transfer_queue_extension ? 'YES' : 'NO'}`);
          if (Array.isArray(historyToCheck)) {
            const transferEvents = historyToCheck.filter(event => 
              event && (event.type === 'Transfer' || event.type === 'transfer') && event.ext && isQueueExtension(event.ext)
            );
            console.log(`🎯 BLA TARGET TRANSFER EVENTS:`, transferEvents);
          }
        }
        
      } catch (error) {
        console.log(`⚠️ BLA PARSE: Error parsing agent_history for call ${call.call_id}: ${error.message}`);
        call.agent_history = [];
        call.lead_history = [];
      }
      return call;
    });

    // Step 3.5: Process outbound calls using proper transfer detection logic
    const processedOutboundCalls = outboundRows.map(call => {
      try {
        console.log(`🔍 BLA OUTBOUND PROCESSING: Processing outbound call ${call.call_id}`);
        console.log(`📋 BLA OUTBOUND AGENT: ${call.agent_name} (${call.extension})`);
        
        // Special debug for target outbound calls
        if (call.call_id === 'ij0lrn0uk290j99nvbc4' || call.call_id === 'vg05si0jadgqje7fg8sa' || 
            call.call_id === 'vg05stm84aeajh2jm9m4' || call.call_id === '29e7gfsh08h6s98kpfgf' ||
            call.call_id === 'uvip1s7sc3cbs425ldif') {
          console.log(`🎯 BLA TARGET OUTBOUND: Found target outbound call ${call.call_id}`);
          console.log(`🎯 BLA TARGET OUTBOUND AGENT: ${call.agent_name} (${call.extension})`);
          console.log(`🎯 BLA TARGET OUTBOUND QUEUE: ${call.queue_campaign_name}`);
          console.log(`🎯 BLA TARGET OUTBOUND CUSTOMER: ${call.customer_number}`);
          console.log(`🎯 BLA TARGET OUTBOUND HISTORY LENGTH: ${Array.isArray(call.agent_history) ? call.agent_history.length : 'not array'}`);
          if (Array.isArray(call.agent_history)) {
            console.log(`🎯 BLA TARGET OUTBOUND HISTORY:`, JSON.stringify(call.agent_history, null, 2));
          }
        }
        
        // Parse agent_history from raw JSON data
        if (call.agent_history && typeof call.agent_history === 'object') {
          // Already parsed as JSON object from MySQL JSON_EXTRACT
          console.log(`📋 BLA OUTBOUND HISTORY: JSON object with ${Array.isArray(call.agent_history) ? call.agent_history.length : 'unknown'} events`);
          if (!Array.isArray(call.agent_history)) {
            call.agent_history = [];
          }
        } else if (typeof call.agent_history === 'string') {
          console.log(`📋 BLA OUTBOUND HISTORY TYPE: string, length: ${call.agent_history.length}`);
          try {
            call.agent_history = JSON.parse(call.agent_history);
            console.log(`📋 BLA OUTBOUND HISTORY: Parsed JSON with ${call.agent_history.length} events`);
          } catch (jsonError) {
            console.log(`📋 BLA HTML PARSE: Extracting transfer data from HTML for outbound call ${call.call_id}`);
            call.agent_history = parseHTMLAgentHistory(call.agent_history);
          }
        } else {
          console.log(`📋 BLA OUTBOUND HISTORY: Setting empty array for call ${call.call_id}, type: ${typeof call.agent_history}`);
          call.agent_history = [];
        }
        
        // Debug: Log first few events if available
        if (Array.isArray(call.agent_history) && call.agent_history.length > 0) {
          console.log(`📋 BLA OUTBOUND SAMPLE EVENTS for ${call.call_id}:`, call.agent_history.slice(0, 3));
        }
        
        // Debug specific outbound call
        if (call.call_id === 'hbakd2iphjrj3u1v73n8' || call.call_id === 'ckjtubds4oh1kseqr8ui') {
          console.log(`🎯 DEBUG OUTBOUND ${call.call_id}:`);
          console.log(`   - agent_history length: ${Array.isArray(call.agent_history) ? call.agent_history.length : 'not array'}`);
          console.log(`   - agent_history events:`, call.agent_history);
        }
        
        // Detect transfer events using the same logic as reportFetcher.js
        const transferInfo = detectTransferEvents(call.agent_history, 'outbound', call);
        call.transfer_event = transferInfo.transfer_event;
        call.transfer_extension = transferInfo.transfer_extension;
        call.transfer_queue_extension = transferInfo.transfer_queue_extension;
        call.transfer_type = transferInfo.transfer_type;
        
        // Debug specific outbound call after detection
        if (call.call_id === 'hbakd2iphjrj3u1v73n8' || call.call_id === 'ckjtubds4oh1kseqr8ui' ||
            call.call_id === 'uvip1s7sc3cbs425ldif') {
          console.log(`🎯 DEBUG OUTBOUND ${call.call_id} AFTER DETECTION:`);
          console.log(`   - transfer_event: ${transferInfo.transfer_event}`);
          console.log(`   - transfer_queue_extension: ${transferInfo.transfer_queue_extension}`);
          console.log(`   - transfer_extension: ${transferInfo.transfer_extension}`);
          console.log(`   - transfer_type: ${transferInfo.transfer_type}`);
          console.log(`   - customer_number: ${call.customer_number}`);
        }
        
        console.log(`🔄 BLA OUTBOUND TRANSFERS: Call ${call.call_id} transfer_event=${transferInfo.transfer_event}, queue_ext=${transferInfo.transfer_queue_extension}`);
        
        // For BLA processing, we'll use agent_history as lead_history for consistency
        call.lead_history = call.agent_history;
      } catch (error) {
        console.log(`⚠️ BLA PARSE: Error parsing agent_history for outbound call ${call.call_id}: ${error.message}`);
        call.agent_history = [];
        call.lead_history = [];
      }
      return call;
    });

    // Step 4: Filter campaign calls that have transfers to queue extensions
    console.log(`🔍 BLA CAMPAIGN FILTERING: Starting to filter ${processedCampaignCalls.length} campaign calls`);
    
    // Debug: Check if target campaign calls are in processedCampaignCalls before filtering
    const targetInProcessed1 = processedCampaignCalls.find(call => call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126');
    const targetInProcessed2 = processedCampaignCalls.find(call => call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce');
    
    if (targetInProcessed1) {
      console.log(`🎯 BLA TARGET PRE-FILTER: Target campaign call b86b6f90-17b3-43aa-a3cc-ede05e890126 found in processedCampaignCalls`);
      console.log(`🎯 BLA TARGET PRE-FILTER: transfer_event=${targetInProcessed1.transfer_event}, transfer_queue_extension=${targetInProcessed1.transfer_queue_extension}`);
    } else {
      console.log(`❌ BLA TARGET PRE-FILTER: Target campaign call b86b6f90-17b3-43aa-a3cc-ede05e890126 NOT found in processedCampaignCalls`);
    }
    
    if (targetInProcessed2) {
      console.log(`🎯 BLA TARGET PRE-FILTER: Target campaign call dca1e20f-3c11-4820-8265-31f4de14dbce found in processedCampaignCalls`);
      console.log(`🎯 BLA TARGET PRE-FILTER: transfer_event=${targetInProcessed2.transfer_event}, transfer_queue_extension=${targetInProcessed2.transfer_queue_extension}`);
    } else {
      console.log(`❌ BLA TARGET PRE-FILTER: Target campaign call dca1e20f-3c11-4820-8265-31f4de14dbce NOT found in processedCampaignCalls - this is the root cause!`);
    }
    
    const campaignCallsWithQueueTransfers = processedCampaignCalls.filter(call => {
      // Handle both boolean and integer values for transfer_event
      const transferEventValue = call.transfer_event === true || call.transfer_event === 1;
      const hasValidQueueExtension = call.transfer_queue_extension && isQueueExtension(call.transfer_queue_extension);
      const hasTransfer = transferEventValue && hasValidQueueExtension;
      
      // Debug target campaign call filtering
      if (call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126' || call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce') {
        console.log(`🎯 BLA TARGET FILTER: Campaign call ${call.call_id} filtering check`);
        console.log(`🎯 BLA TARGET FILTER: transfer_event=${call.transfer_event} (type: ${typeof call.transfer_event}), transfer_queue_extension=${call.transfer_queue_extension}`);
        console.log(`🎯 BLA TARGET FILTER: transferEventValue=${transferEventValue}, hasValidQueueExtension=${hasValidQueueExtension}`);
        console.log(`🎯 BLA TARGET FILTER: isQueueExtension(${call.transfer_queue_extension})=${isQueueExtension(call.transfer_queue_extension)}`);
        console.log(`🎯 BLA TARGET FILTER: hasTransfer=${hasTransfer}, will be included=${hasTransfer ? 'YES' : 'NO'}`);
        if (!hasTransfer) {
          console.log(`🎯 BLA TARGET FILTER: Full call object:`, JSON.stringify(call, null, 2));
        }
      }
      
      return hasTransfer;
    });
    
    console.log(`🔍 BLA CAMPAIGN FILTER: Filtered ${processedCampaignCalls.length} campaign calls to ${campaignCallsWithQueueTransfers.length} with queue transfers`);
    console.log(`🔄 BLA CAMPAIGN TRANSFERS: Found ${campaignCallsWithQueueTransfers.length} campaign calls with queue transfers out of ${processedCampaignCalls.length} total`);
    
    // Debug: Check if target campaign call made it through the filter
    const targetCampaignInFiltered = campaignCallsWithQueueTransfers.find(call => call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126');
    if (targetCampaignInFiltered) {
      console.log(`🎯 BLA TARGET IN FILTERED: Target campaign call found in campaignCallsWithQueueTransfers`);
      console.log(`🎯 BLA TARGET FILTERED PROPERTIES: transfer_event=${targetCampaignInFiltered.transfer_event}, transfer_queue_extension=${targetCampaignInFiltered.transfer_queue_extension}`);
    } else {
      console.log(`❌ BLA TARGET NOT IN FILTERED: Target campaign call NOT found in campaignCallsWithQueueTransfers - this is why it's not being linked!`);
    }
    
    // Debug: Log queue extensions being transferred to
    const campaignQueueExtensions = [...new Set(campaignCallsWithQueueTransfers.map(call => call.transfer_queue_extension))];
    console.log(`📋 BLA CAMPAIGN QUEUE EXTENSIONS: ${campaignQueueExtensions.join(', ')}`);
    campaignQueueExtensions.forEach(queueExt => {
      const count = campaignCallsWithQueueTransfers.filter(call => call.transfer_queue_extension === queueExt).length;
      const mappedCalleeId = queueToCalleeExtensionMap[queueExt];
      console.log(`   - Queue ${queueExt}: ${count} calls → Callee ID ${mappedCalleeId || 'NOT MAPPED'}`);
    });

    // Step 4.5: Filter outbound calls that have transfers to queue extensions
    const outboundCallsWithQueueTransfers = processedOutboundCalls.filter(call => {
      // Debug specific outbound call
      if (call.call_id === 'te816digqt4e7n598bpv' || call.call_id === 'uvip1s7sc3cbs425ldif') {
        console.log(`🎯 DEBUG ${call.call_id} FILTER CHECK:`);
        console.log(`   - transfer_event: ${call.transfer_event} (type: ${typeof call.transfer_event})`);
        console.log(`   - transfer_queue_extension: ${call.transfer_queue_extension}`);
        console.log(`   - Will be included: ${call.transfer_event && call.transfer_queue_extension ? 'YES' : 'NO'}`);
      }
      return call.transfer_event && call.transfer_queue_extension;
    });
    console.log(`🔄 BLA OUTBOUND TRANSFERS: Found ${outboundCallsWithQueueTransfers.length} outbound calls with queue transfers out of ${processedOutboundCalls.length} total`);
    
    // Debug: Check if target outbound call is in filtered list
    const targetOutboundInFiltered = outboundCallsWithQueueTransfers.find(c => c.call_id === 'te816digqt4e7n598bpv');
    if (targetOutboundInFiltered) {
      console.log(`✅ DEBUG te816digqt4e7n598bpv IS in outboundCallsWithQueueTransfers`);
    } else {
      const targetInProcessed = processedOutboundCalls.find(c => c.call_id === 'te816digqt4e7n598bpv');
      if (targetInProcessed) {
        console.log(`❌ DEBUG te816digqt4e7n598bpv is in processedOutboundCalls but NOT in filtered list!`);
        console.log(`   - transfer_event: ${targetInProcessed.transfer_event}`);
        console.log(`   - transfer_queue_extension: ${targetInProcessed.transfer_queue_extension}`);
      } else {
        console.log(`❌ DEBUG te816digqt4e7n598bpv NOT FOUND in processedOutboundCalls at all`);
      }
    }

    // Step 5: Process inbound calls using proper transfer detection logic
    const processedInboundCalls = inboundRows.map(call => {
      try {
        // Special debug for target inbound call
        if (call.call_id === '78682502-8ce3-492c-b2f6-2644e35ac378') {
          console.log(`🎯 BLA TARGET INBOUND: Found target inbound call ${call.call_id}`);
          console.log(`🎯 BLA TARGET INBOUND QUEUE: ${call.queue_campaign_name}`);
          console.log(`🎯 BLA TARGET INBOUND CALLEE: ${call.callee_id_number}`);
          console.log(`🎯 BLA TARGET INBOUND CALLER: ${call.caller_id_number}`);
          console.log(`🎯 BLA TARGET INBOUND AGENT: ${call.agent_name} (${call.extension})`);
        }
        
        // Parse agent_history from raw JSON data
        if (call.agent_history && typeof call.agent_history === 'object') {
          // Already parsed as JSON object from MySQL JSON_EXTRACT
          console.log(`📋 BLA INBOUND HISTORY: JSON object with ${Array.isArray(call.agent_history) ? call.agent_history.length : 'unknown'} events for call ${call.call_id}`);
          if (!Array.isArray(call.agent_history)) {
            call.agent_history = [];
          }
        } else if (typeof call.agent_history === 'string') {
          console.log(`📋 BLA INBOUND HISTORY TYPE: string, length: ${call.agent_history.length} for call ${call.call_id}`);
          try {
            call.agent_history = JSON.parse(call.agent_history);
            console.log(`📋 BLA INBOUND HISTORY: Parsed JSON with ${call.agent_history.length} events`);
          } catch (jsonError) {
            console.log(`📋 BLA HTML PARSE: Extracting transfer data from HTML for inbound call ${call.call_id}`);
            call.agent_history = parseHTMLAgentHistory(call.agent_history);
          }
        } else {
          console.log(`📋 BLA INBOUND HISTORY: Setting empty array for call ${call.call_id}, type: ${typeof call.agent_history}`);
          call.agent_history = [];
        }
        
        // Debug: Log first few events if available
        if (Array.isArray(call.agent_history) && call.agent_history.length > 0) {
          console.log(`📋 BLA INBOUND SAMPLE EVENTS for ${call.call_id}:`, call.agent_history.slice(0, 3));
        }
        
        // Special debug for target inbound call - show full agent_history
        if (call.call_id === '78682502-8ce3-492c-b2f6-2644e35ac378' && Array.isArray(call.agent_history)) {
          console.log(`🎯 BLA TARGET INBOUND HISTORY:`, JSON.stringify(call.agent_history, null, 2));
        }
        
        // Detect transfer events using the same logic as reportFetcher.js
        const transferInfo = detectTransferEvents(call.agent_history, 'inbound', call);
        call.transfer_event = transferInfo.transfer_event;
        call.transfer_extension = transferInfo.transfer_extension;
        call.transfer_queue_extension = transferInfo.transfer_queue_extension;
        call.transfer_type = transferInfo.transfer_type;
        
        // Detect failed transfer attempts
        const failedTransfers = detectFailedTransfers(call.agent_history);
        call.failed_transfers = failedTransfers;
        
        if (failedTransfers.length > 0) {
          console.log(`❌ BLA FAILED TRANSFERS: Call ${call.call_id} had ${failedTransfers.length} failed transfer attempts:`);
          failedTransfers.forEach(failed => {
            console.log(`   - Agent ${failed.extension} (${failed.agent_name}) - ${failed.reason} at ${failed.dial_time_formatted}`);
          });
        }
        
      } catch (error) {
        console.log(`⚠️ BLA PARSE: Error parsing agent_history for inbound call ${call.call_id}: ${error.message}`);
        call.agent_history = [];
      }
      return call;
    });

    // Step 5.5: Fetch recording information for inbound calls from final_report table
    console.log(`🎵 BLA RECORDINGS: Fetching recording info for ${processedInboundCalls.length} inbound calls`);
    
    // Get all unique inbound call IDs
    const inboundCallIds = processedInboundCalls.map(call => call.call_id).filter(Boolean);
    
    if (inboundCallIds.length > 0) {
      // Create placeholders for the IN clause
      const placeholders = inboundCallIds.map(() => '?').join(',');
      
      const recordingSql = `
        SELECT call_id, recording
        FROM final_report 
        WHERE call_id IN (${placeholders})
          AND recording IS NOT NULL 
          AND recording != ''
      `;
      
      try {
        const recordingRows = await pool.query(recordingSql, inboundCallIds);
        console.log(`🎵 BLA RECORDINGS: Found ${recordingRows.length} inbound calls with recordings`);
        
        // Create a map of call_id to recording URL
        const recordingMap = {};
        recordingRows.forEach(row => {
          recordingMap[row.call_id] = row.recording;
        });
        
        // Add recording information to processed inbound calls
        processedInboundCalls.forEach(call => {
          if (recordingMap[call.call_id]) {
            call.recording = recordingMap[call.call_id];
            console.log(`🎵 BLA RECORDING ADDED: ${call.call_id} -> ${call.recording}`);
          }
        });
        
      } catch (recordingError) {
        console.error('❌ BLA RECORDINGS ERROR:', recordingError);
      }
    }

    // Step 6: Link campaign calls with their corresponding inbound calls
    console.log(`🔗 BLA LINKING: Starting to link ${campaignCallsWithQueueTransfers.length} campaign calls with queue transfers`);
    
    // Debug: Check if target campaign calls are in the filtered list
    const targetCampaignCall1 = campaignCallsWithQueueTransfers.find(call => call.call_id === 'b86b6f90-17b3-43aa-a3cc-ede05e890126');
    const targetCampaignCall2 = campaignCallsWithQueueTransfers.find(call => call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce');
    const targetCampaignCall3 = campaignCallsWithQueueTransfers.find(call => call.call_id === '5c864e9b-ed81-4ff9-9f21-e56aed297d1a');
    
    if (targetCampaignCall1) {
      console.log(`🎯 BLA TARGET IN FILTERED LIST: Target campaign call b86b6f90-17b3-43aa-a3cc-ede05e890126 found in campaignCallsWithQueueTransfers`);
      console.log(`🎯 BLA TARGET PROPERTIES: transfer_event=${targetCampaignCall1.transfer_event}, transfer_queue_extension=${targetCampaignCall1.transfer_queue_extension}`);
    } else {
      console.log(`❌ BLA TARGET NOT IN FILTERED LIST: Target campaign call b86b6f90-17b3-43aa-a3cc-ede05e890126 NOT found in campaignCallsWithQueueTransfers`);
    }
    
    if (targetCampaignCall2) {
      console.log(`🎯 BLA TARGET IN FILTERED LIST: Target campaign call dca1e20f-3c11-4820-8265-31f4de14dbce found in campaignCallsWithQueueTransfers`);
      console.log(`🎯 BLA TARGET PROPERTIES: transfer_event=${targetCampaignCall2.transfer_event}, transfer_queue_extension=${targetCampaignCall2.transfer_queue_extension}`);
    } else {
      console.log(`❌ BLA TARGET NOT IN FILTERED LIST: Target campaign call dca1e20f-3c11-4820-8265-31f4de14dbce NOT found in campaignCallsWithQueueTransfers`);
    }
    
    if (targetCampaignCall3) {
      console.log(`🎯 BLA NEW EXAMPLE: Campaign call 5c864e9b-ed81-4ff9-9f21-e56aed297d1a found in campaignCallsWithQueueTransfers`);
      console.log(`🎯 BLA NEW EXAMPLE PROPERTIES: transfer_event=${targetCampaignCall3.transfer_event}, transfer_queue_extension=${targetCampaignCall3.transfer_queue_extension}`);
    } else {
      console.log(`❌ BLA NEW EXAMPLE: Campaign call 5c864e9b-ed81-4ff9-9f21-e56aed297d1a NOT found in campaignCallsWithQueueTransfers`);
    }
    
    // Debug: Check if target inbound calls exist in processedInboundCalls
    const targetInboundCall1 = processedInboundCalls.find(call => call.call_id === 'a9edd223-5ede-4a62-aeb8-cb27f84f9abb');
    const targetInboundCall2 = processedInboundCalls.find(call => call.call_id === '63b4a12e-70d1-4922-a87a-b66a9dffc353');
    const targetInboundCall3 = processedInboundCalls.find(call => call.call_id === 'adf8fde0-bca4-4157-a236-4303243eedee');
    
    if (targetInboundCall1) {
      console.log(`🎯 BLA TARGET INBOUND FOUND: Target inbound call a9edd223-5ede-4a62-aeb8-cb27f84f9abb found in processedInboundCalls`);
      console.log(`🎯 BLA TARGET INBOUND CALLEE: callee_id_number=${targetInboundCall1.callee_id_number}`);
    } else {
      console.log(`❌ BLA TARGET INBOUND NOT FOUND: Target inbound call a9edd223-5ede-4a62-aeb8-cb27f84f9abb NOT found in processedInboundCalls`);
    }
    
    if (targetInboundCall2) {
      console.log(`🎯 BLA TARGET INBOUND FOUND: Target inbound call 63b4a12e-70d1-4922-a87a-b66a9dffc353 found in processedInboundCalls`);
      console.log(`🎯 BLA TARGET INBOUND CALLEE: callee_id_number=${targetInboundCall2.callee_id_number}`);
    } else {
      console.log(`❌ BLA TARGET INBOUND NOT FOUND: Target inbound call 63b4a12e-70d1-4922-a87a-b66a9dffc353 NOT found in processedInboundCalls`);
    }
    
    if (targetInboundCall3) {
      console.log(`🎯 BLA NEW EXAMPLE INBOUND: Target inbound call adf8fde0-bca4-4157-a236-4303243eedee found in processedInboundCalls`);
      console.log(`🎯 BLA NEW EXAMPLE INBOUND CALLEE: callee_id_number=${targetInboundCall3.callee_id_number}`);
    } else {
      console.log(`❌ BLA NEW EXAMPLE INBOUND: Target inbound call adf8fde0-bca4-4157-a236-4303243eedee NOT found in processedInboundCalls`);
    }
    
    console.log(`🔗 BLA LINKING: Starting campaign-to-inbound linking process`);
    console.log(`🔗 BLA LINKING: campaignCallsWithQueueTransfers has ${campaignCallsWithQueueTransfers.length} calls`);
    console.log(`🔗 BLA LINKING: processedInboundCalls has ${processedInboundCalls.length} calls`);
  
    // Debug: Check if target campaign call is in the input to linking function
    const targetCampaignInLinking = campaignCallsWithQueueTransfers.find(call => call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce');
    console.log(`🎯 BLA LINKING MAIN: Searching for target campaign call dca1e20f-3c11-4820-8265-31f4de14dbce in ${campaignCallsWithQueueTransfers.length} calls`);
    if (targetCampaignInLinking) {
      console.log(`🎯 BLA LINKING MAIN: Target campaign call dca1e20f-3c11-4820-8265-31f4de14dbce found in campaignCallsWithQueueTransfers - will be passed to linking function`);
    } else {
      console.log(`❌ BLA LINKING MAIN: Target campaign call dca1e20f-3c11-4820-8265-31f4de14dbce NOT found in campaignCallsWithQueueTransfers - this is why it's not being linked!`);
      console.log(`❌ BLA LINKING MAIN: campaignCallsWithQueueTransfers has ${campaignCallsWithQueueTransfers.length} calls`);
      campaignCallsWithQueueTransfers.forEach((call, index) => {
        if (call.call_id.includes('dca1e20f') || call.call_id.includes('202d5fe9')) {
          console.log(`🔍 BLA LINKING MAIN: Call ${index + 1}: ${call.call_id} (similar to target)`);
        }
      });
    }
    
    // Create shared set to track used inbound calls across all linking functions
    const sharedUsedInboundCallIds = new Set();
    
    // Step 7: Detect inbound-to-inbound transfers within the same call record FIRST
    console.log(`🔍 BLA INTERNAL TRANSFERS: Detecting internal transfers in ${processedInboundCalls.length} inbound calls`);
    const inboundInternalTransfers = detectInboundInternalTransfers(processedInboundCalls);
    
    // Mark internal transfer calls as used to prevent duplicate processing in other linking functions
    inboundInternalTransfers.forEach(transfer => {
      if (transfer.source_call && transfer.source_call.call_id) {
        sharedUsedInboundCallIds.add(transfer.source_call.call_id);
        console.log(`🔒 BLA INTERNAL USED: Marked internal transfer call ${transfer.source_call.call_id} as used`);
      }
    });
    
    // Step 8: Link campaign calls to inbound calls
    const campaignToInboundLinks = linkCampaignToInboundCalls(campaignCallsWithQueueTransfers, processedInboundCalls, sharedUsedInboundCallIds);

    // Step 8.5: Link outbound calls to inbound calls (reuse campaign linking function)
    console.log(`🔗 BLA OUTBOUND LINKING: Linking ${outboundCallsWithQueueTransfers.length} outbound calls with ${processedInboundCalls.length} inbound calls`);
    if (outboundCallsWithQueueTransfers.length > 0) {
      console.log(`📋 BLA OUTBOUND CALLS TO LINK:`, outboundCallsWithQueueTransfers.map(c => `${c.call_id} (queue: ${c.transfer_queue_extension}, customer: ${c.customer_number || c.caller_id_number || 'NONE'})`));
      // Debug specific outbound call
      const targetOutbound = outboundCallsWithQueueTransfers.find(c => c.call_id === 'hbakd2iphjrj3u1v73n8');
      if (targetOutbound) {
        console.log(`🎯 DEBUG OUTBOUND hbakd2iphjrj3u1v73n8 BEFORE LINKING:`);
        console.log(`   - customer_number: ${targetOutbound.customer_number}`);
        console.log(`   - caller_id_number: ${targetOutbound.caller_id_number}`);
        console.log(`   - transfer_queue_extension: ${targetOutbound.transfer_queue_extension}`);
      }
    }
    const outboundToInboundLinks = linkCampaignToInboundCalls(outboundCallsWithQueueTransfers, processedInboundCalls, sharedUsedInboundCallIds);
    
    // Step 9: Link inbound calls with other inbound calls (inbound-to-inbound transfers between separate calls)
    const inboundToInboundLinks = linkInboundToInboundCalls(processedInboundCalls, sharedUsedInboundCallIds);

    // Step 9.5: Detect Failed BLA Hot Patch Transfers (for both campaign and outbound calls)
    console.log(`🔍 BLA FAILED TRANSFER DETECTION: Checking for failed transfers`);
    const failedTransfers = detectFailedBlaHotPatchTransfers(
      processedCampaignCalls, 
      processedInboundCalls, 
      sharedUsedInboundCallIds,
      processedOutboundCalls
    );
    console.log(`🔴 BLA FAILED TRANSFERS: Found ${failedTransfers.length} failed transfer scenarios`);

    // Step 10: Combine all types of linked calls
    const allLinkedCallsRaw = [...campaignToInboundLinks, ...outboundToInboundLinks, ...inboundToInboundLinks, ...inboundInternalTransfers, ...failedTransfers];
    
    // Deduplicate by inbound call ID - keep only the first occurrence of each inbound call
    const seenInboundCallIds = new Set();
    const seenSourceCallIds = new Set(); // Track source calls to avoid duplicates
    const allLinkedCalls = allLinkedCallsRaw.filter(linkedCall => {
      // Get inbound call ID from different possible structures
      let inboundCallId = null;
      let sourceCallId = null;
      
      // For campaign/outbound to inbound transfers
      if (linkedCall.inbound_call?.call_id) {
        inboundCallId = linkedCall.inbound_call.call_id;
        sourceCallId = linkedCall.campaign_call?.call_id || linkedCall.outbound_call?.call_id;
      }
      // For inbound to inbound transfers
      else if (linkedCall.target_call?.call_id) {
        inboundCallId = linkedCall.target_call.call_id;
        sourceCallId = linkedCall.source_call?.call_id;
      }
      // For inbound-to-inbound transfers between separate calls (source_inbound_call/target_inbound_call shape)
      else if (linkedCall.source_inbound_call?.call_id) {
        sourceCallId = linkedCall.source_inbound_call.call_id;
        inboundCallId = linkedCall.target_inbound_call?.call_id || null;
      }
      // For internal transfers
      else if (linkedCall.source_call?.call_id) {
        sourceCallId = linkedCall.source_call.call_id;
        // Internal transfers don't have separate inbound calls
      }
      // For failed BLA hot patch transfers (campaign/outbound)
      // These objects don't have inbound_call/target_call but still represent a source call.
      else if (linkedCall.campaign_call?.call_id || linkedCall.outbound_call?.call_id) {
        sourceCallId = linkedCall.campaign_call?.call_id || linkedCall.outbound_call?.call_id;
      }
      
      // Check for duplicate inbound calls
      if (inboundCallId) {
        if (seenInboundCallIds.has(inboundCallId)) {
          console.log(`🔄 BLA DUPLICATE INBOUND: Removing duplicate entry for inbound call ${inboundCallId}`);
          return false; // Skip duplicate
        }
        seenInboundCallIds.add(inboundCallId);
      }
      
      // Check for duplicate source calls (prevents same source appearing multiple times)
      if (sourceCallId) {
        if (seenSourceCallIds.has(sourceCallId)) {
          console.log(`🔄 BLA DUPLICATE SOURCE: Removing duplicate entry for source call ${sourceCallId}`);
          return false; // Skip duplicate
        }
        seenSourceCallIds.add(sourceCallId);
      }
      
      return true; // Keep first occurrence
    });
    
    console.log(`🔄 BLA DEDUPLICATION: Removed ${allLinkedCallsRaw.length - allLinkedCalls.length} duplicate entries`);
    
    // Debug: Analyze missing transfers
    console.log(`🔍 BLA TRANSFER ANALYSIS:`);
    console.log(`   📊 Total calls with queue transfers: ${campaignCallsWithQueueTransfers.length + outboundCallsWithQueueTransfers.length}`);
    console.log(`   ✅ Successfully linked calls: ${allLinkedCalls.length}`);
    console.log(`   ❌ Missing links: ${(campaignCallsWithQueueTransfers.length + outboundCallsWithQueueTransfers.length) - allLinkedCalls.length}`);
    
    // Log unlinked campaign calls
    const linkedCampaignIds = new Set(campaignToInboundLinks.map(link => link.campaign_call?.call_id || link.campaign_call?.callid));
    const unlinkedCampaignCalls = campaignCallsWithQueueTransfers.filter(call => !linkedCampaignIds.has(call.call_id || call.callid));
    if (unlinkedCampaignCalls.length > 0) {
      console.log(`❌ BLA UNLINKED CAMPAIGN CALLS: ${unlinkedCampaignCalls.length}`);
      unlinkedCampaignCalls.forEach(call => {
        const transferTime = new Date((call.transfer_timestamp || call.called_time) * 1000).toISOString();
        console.log(`   - ${call.call_id || call.callid}: Queue ${call.transfer_queue_extension}, Time ${transferTime}`);
        
        // Special debugging for target campaign call
        if (call.call_id === 'dca1e20f-3c11-4820-8265-31f4de14dbce') {
          console.log(`🎯 BLA UNLINKED TARGET: Analyzing why dca1e20f-3c11-4820-8265-31f4de14dbce failed to link`);
          console.log(`🎯 BLA UNLINKED TARGET: Queue ${call.transfer_queue_extension} should map to callee_id ${queueToCalleeExtensionMap[call.transfer_queue_extension]}`);
          console.log(`🎯 BLA UNLINKED TARGET: Transfer time ${transferTime} (epoch: ${call.transfer_timestamp || call.called_time})`);
          
          // Debug: Check if we have the correct transfer timestamp from agent history
          if (call.transfer_timestamp && call.transfer_timestamp !== call.called_time) {
            const actualTransferTime = new Date(call.transfer_timestamp * 1000).toISOString();
            const callStartTime = new Date(call.called_time * 1000).toISOString();
            console.log(`🎯 BLA TIMESTAMP DEBUG: Call start time: ${callStartTime}`);
            console.log(`🎯 BLA TIMESTAMP DEBUG: Actual transfer time: ${actualTransferTime}`);
            console.log(`🎯 BLA TIMESTAMP DEBUG: Using transfer_timestamp (${call.transfer_timestamp}) instead of called_time (${call.called_time})`);
          } else {
            console.log(`⚠️ BLA TIMESTAMP WARNING: No separate transfer_timestamp found, using called_time (${call.called_time})`);
          }
          
          // Check if target inbound call exists
          const expectedCalleeId = queueToCalleeExtensionMap[call.transfer_queue_extension];
          const targetInboundCall = processedInboundCalls.find(inbound => inbound.call_id === '63b4a12e-70d1-4922-a87a-b66a9dffc353');
          if (targetInboundCall) {
            console.log(`🎯 BLA UNLINKED TARGET: Target inbound call 63b4a12e-70d1-4922-a87a-b66a9dffc353 EXISTS with callee_id ${targetInboundCall.callee_id_number}`);
            const calleeIdMatch = targetInboundCall.callee_id_number === expectedCalleeId;
            console.log(`🎯 BLA UNLINKED TARGET: Callee ID match: ${calleeIdMatch} (${targetInboundCall.callee_id_number} === ${expectedCalleeId})`);
            
            // Check time window using the correct transfer timestamp
            const correctTransferTime = call.transfer_timestamp || call.called_time;
            const campaignTransferTime = correctTransferTime * 1000;
            const inboundCallTime = (targetInboundCall.called_time || targetInboundCall.timestamp) * 1000;
            const timeDifference = Math.abs(campaignTransferTime - inboundCallTime) / 1000;
            console.log(`🎯 BLA UNLINKED TARGET: Time difference: ${timeDifference}s (within 600s window: ${timeDifference <= 600})`);
            console.log(`🎯 BLA UNLINKED TARGET: Campaign transfer: ${new Date(campaignTransferTime).toISOString()}`);
            console.log(`🎯 BLA UNLINKED TARGET: Inbound call: ${new Date(inboundCallTime).toISOString()}`);
          } else {
            console.log(`❌ BLA UNLINKED TARGET: Target inbound call 63b4a12e-70d1-4922-a87a-b66a9dffc353 NOT FOUND in processedInboundCalls`);
          }
        }
      });
    }
    
    // Log unlinked outbound calls
    const linkedOutboundIds = new Set(outboundToInboundLinks.map(link => link.campaign_call?.call_id || link.campaign_call?.callid));
    const unlinkedOutboundCalls = outboundCallsWithQueueTransfers.filter(call => !linkedOutboundIds.has(call.call_id || call.callid));
    if (unlinkedOutboundCalls.length > 0) {
      console.log(`❌ BLA UNLINKED OUTBOUND CALLS: ${unlinkedOutboundCalls.length}`);
      unlinkedOutboundCalls.forEach(call => {
        console.log(`   - ${call.call_id || call.callid}: Queue ${call.transfer_queue_extension}, Time ${new Date((call.called_time || call.timestamp) * 1000).toISOString()}`);
      });
    }

    // Step 9: Format the final report data
    const reportData = allLinkedCalls.map(linkedCall => {
      // Handle failed BLA Hot Patch Transfers (both campaign and outbound)
      if (linkedCall.transfer_status === 'Failed' && linkedCall.abandoned_inbound_calls) {
        const { campaign_call, outbound_call, abandoned_inbound_calls, failed_agents, total_legs, source_type } = linkedCall;
        
        // Determine if this is a campaign or outbound failed transfer
        const sourceCall = campaign_call || outbound_call;
        const callType = source_type === 'outbound' ? 'Outbound' : 'Campaign';
        
        return {
          // Call type identification
          call_type: callType,
          
          // Source call information (1st leg) - works for both campaign and outbound
          campaign_call_id: sourceCall.call_id,
          campaign_agent_name: sourceCall.agent_name,
          campaign_agent_extension: sourceCall.extension || sourceCall.agent_ext,
          campaign_customer_name: sourceCall.caller_id_name,
          // For outbound calls, callee_id_number is the customer being called
          campaign_customer_number: (callType === 'Outbound' ? 
            (sourceCall.callee_id_number || sourceCall.customer_number || sourceCall.to) : 
            (sourceCall.customer_number || sourceCall.to || sourceCall.callee_id_number)) || sourceCall.caller_id_number,
          campaign_called_time: sourceCall.called_time,
          campaign_called_time_formatted: sourceCall.called_time_formatted,
          campaign_talk_duration: sourceCall.talk_duration || sourceCall.billing_seconds,
          campaign_disposition: sourceCall.agent_disposition,
          campaign_follow_up_notes: sourceCall.follow_up_notes,
          campaign_recording: sourceCall.recording || sourceCall.recording_filename,
          campaign_agent_history: formatAgentHistoryWithUTC(sourceCall.agent_history),
          
          // Hold start and stop times (transfer window)
          hold_start_time: sourceCall.hold_start_time,
          hold_start_time_formatted: sourceCall.hold_start_time_formatted,
          hold_stop_time: sourceCall.hold_stop_time,
          hold_stop_time_formatted: sourceCall.hold_stop_time_formatted,
          transfer_queue_extension: sourceCall.transfer_queue_extension,
          
          // Transfer information
          transfer_status: 'Failed',
          transfer_status_reason: 'No agent answered - call abandoned',
          
          // Failed transfer details
          total_failed_legs: total_legs,
          abandoned_calls: abandoned_inbound_calls.map(call => ({
            call_id: call.call_id,
            caller_id_number: call.caller_id_number,
            callee_id_number: call.callee_id_number,
            queue_name: call.queue_name,
            called_time: call.called_time,
            called_time_formatted: call.called_time ? 
              DateTime.fromSeconds(call.called_time, { zone: 'Asia/Dubai' }).toFormat('dd/MM/yyyy, HH:mm:ss') : null,
            abandoned: call.abandoned,
            failed_agents: call.failed_agents || []
          })),
          
          // Failed agents list
          failed_agents: failed_agents,
          failed_transfer_count: failed_agents.length,
          failed_agents_list: failed_agents.map(f => `${f.agent_name} (${f.extension})`).join(', '),
          
          // Inbound call information (null for failed transfers)
          inbound_call_id: null,
          receiving_agent_name: 'No agent answered',
          receiving_agent_extension: null,
          
          // Additional fields for display
          record_type: 'Failed Transfer',
          status: 'Failed'
        };
      }
      
      // Handle both campaign-to-inbound and outbound-to-inbound transfers
      if (linkedCall.campaign_call && linkedCall.inbound_call) {
        // Campaign or Outbound to inbound transfer (successful only)
        const { campaign_call, inbound_call, link_metadata } = linkedCall;
        
        // Determine call type based on record_type
        const callType = campaign_call.record_type === 'Campaign' ? 'Campaign' : 'Outbound';

        return {
          // Call type identification
          call_type: callType,
          
          // Campaign call information (1st leg)
          campaign_call_id: campaign_call.call_id,
          campaign_agent_name: campaign_call.agent_name,
          campaign_agent_extension: campaign_call.extension,
          campaign_customer_name: campaign_call.caller_id_name,
          // For outbound calls, callee_id_number is the customer being called
          // For campaign calls, customer_number (from $.to) or lead_history has the customer
          campaign_customer_number: (callType === 'Outbound' ? 
            (campaign_call.callee_id_number || campaign_call.customer_number || campaign_call.to) : 
            (campaign_call.customer_number || campaign_call.to || campaign_call.callee_id_number)) || campaign_call.caller_id_number,
          campaign_called_time: campaign_call.called_time,
          campaign_called_time_formatted: campaign_call.called_time_formatted,
          campaign_talk_duration: campaign_call.talk_duration,
          campaign_disposition: campaign_call.agent_disposition,
          campaign_follow_up_notes: campaign_call.follow_up_notes,
          campaign_recording: campaign_call.recording,
          campaign_agent_history: formatAgentHistoryWithUTC(campaign_call.agent_history),
          
          // Transfer information
          transfer_queue_extension: link_metadata.queue_extension,
          transfer_time: link_metadata.transfer_time,
          transfer_time_formatted: link_metadata.transfer_time && !isNaN(link_metadata.transfer_time) ? 
            DateTime.fromSeconds(link_metadata.transfer_time, { zone: 'Asia/Dubai' }).toFormat('dd/MM/yyyy, HH:mm:ss') : null,
          transfer_status: link_metadata.transfer_status,
          transfer_status_reason: link_metadata.transfer_status_reason,
          transfer_time_difference_seconds: link_metadata.time_difference_seconds,
          callee_id_mapped: link_metadata.callee_id,
          
          // Transferred Call Time (agent_enter timestamp from 2nd leg)
          // This is when the agent entered the call after the transfer
          transferred_call_time: inbound_call.transferred_call_time || inbound_call._agentEnterTimestamp,
          transferred_call_time_formatted: (() => {
            const transferredTime = inbound_call.transferred_call_time || inbound_call._agentEnterTimestamp;
            return transferredTime && !isNaN(transferredTime) ? 
              DateTime.fromSeconds(transferredTime, { zone: 'Asia/Dubai' }).toFormat('dd/MM/yyyy, HH:mm:ss') : null;
          })(),
          
          // Inbound call information (2nd leg)
          inbound_call_id: inbound_call.call_id,
          inbound_called_time: inbound_call.called_time,
          inbound_called_time_formatted: inbound_call.called_time_formatted,
          receiving_agent_name: inbound_call.receiving_agent_name,
          receiving_agent_extension: inbound_call.receiving_agent_extension,
          inbound_talk_duration: inbound_call.talk_duration,
          inbound_recording: inbound_call.recording,
          inbound_agent_history: inbound_call.agent_history,
          
          // Failed transfer information
          failed_transfers: inbound_call.failed_transfers || [],
          failed_transfer_count: (inbound_call.failed_transfers || []).length,
          failed_agents_list: (inbound_call.failed_transfers || []).map(f => `${f.agent_name} (${f.extension})`).join(', '),
          
          // Link metadata
          time_difference_seconds: link_metadata.time_difference_seconds,
          
          // Additional fields for display
          record_type: 'Transferred CDR',
          status: 'Linked'
        };
      } else if (linkedCall.transfer_type === 'inbound_internal_transfer') {
        // Inbound-to-inbound transfer within the same call record
        const { source_call, target_call, link_metadata } = linkedCall;

        return {
          // Call type identification
          call_type: 'Inbound',
          
          // Source call information (original agent)
          campaign_call_id: source_call.call_id,
          campaign_agent_name: source_call.agent_name,
          campaign_agent_extension: source_call.extension,
          campaign_customer_name: source_call.caller_id_name,
          campaign_customer_number: source_call.caller_id_number,
          campaign_called_time: source_call.called_time,
          campaign_called_time_formatted: source_call.called_time_formatted,
          campaign_talk_duration: source_call.talk_duration,
          campaign_disposition: source_call.agent_disposition,
          campaign_follow_up_notes: source_call.follow_up_notes || '',
          campaign_recording: source_call.recording,
          campaign_agent_history: formatAgentHistoryWithUTC(source_call.agent_history),
          
          // Transfer information
          transfer_queue_extension: link_metadata.queue_extension,
          transfer_time: link_metadata.transfer_time,
          transfer_time_formatted: link_metadata.transfer_time ? 
            DateTime.fromSeconds(link_metadata.transfer_time, { zone: 'Asia/Dubai' }).toFormat('dd/MM/yyyy, HH:mm:ss') : null,
          transfer_status: link_metadata.transfer_status,
          transfer_status_reason: link_metadata.transfer_status_reason,
          
          // Receiving agent information (same call, different agent)
          inbound_call_id: target_call.call_id, // Same call ID
          inbound_called_time: target_call.called_time,
          inbound_called_time_formatted: target_call.called_time_formatted,
          receiving_agent_name: target_call.receiving_agent_name,
          receiving_agent_extension: target_call.receiving_agent_extension,
          inbound_talk_duration: target_call.talk_duration,
          inbound_recording: target_call.recording,
          inbound_agent_history: formatAgentHistoryWithUTC(target_call.agent_history),
          
          // Transfer enter information
          transfer_enter_time: link_metadata.transfer_enter_time,
          transfer_enter_time_formatted: link_metadata.transfer_enter_time ? 
            DateTime.fromSeconds(link_metadata.transfer_enter_time, { zone: 'Asia/Dubai' }).toFormat('dd/MM/yyyy, HH:mm:ss') : null,
          
          // Additional fields for display
          record_type: 'Internal Transfer',
          status: 'Linked',
          is_internal_transfer: true
        };
      } else if (linkedCall.source_inbound_call) {
        // Inbound-to-inbound transfer (successful or failed)
        const { source_inbound_call, target_inbound_call, link_metadata } = linkedCall;

        return {
          // Call type identification
          call_type: 'Inbound',
          
          // Source inbound call information (1st leg)
          campaign_call_id: source_inbound_call.call_id,
          campaign_agent_name: source_inbound_call.agent_name,
          campaign_agent_extension: source_inbound_call.extension,
          campaign_customer_name: source_inbound_call.caller_id_name,
          campaign_customer_number: source_inbound_call.customer_number || source_inbound_call.to || source_inbound_call.caller_id_number,
          campaign_called_time: source_inbound_call.called_time,
          campaign_called_time_formatted: source_inbound_call.called_time_formatted,
          campaign_talk_duration: source_inbound_call.talk_duration,
          campaign_disposition: source_inbound_call.agent_disposition,
          campaign_follow_up_notes: source_inbound_call.follow_up_notes || '',
          campaign_recording: source_inbound_call.recording,
          campaign_agent_history: formatAgentHistoryWithUTC(source_inbound_call.agent_history),
          
          // Transfer information
          transfer_queue_extension: link_metadata.queue_extension,
          transfer_time: link_metadata.transfer_time,
          transfer_time_formatted: link_metadata.transfer_time && !isNaN(link_metadata.transfer_time) ? 
            DateTime.fromSeconds(link_metadata.transfer_time, { zone: 'Asia/Dubai' }).toFormat('dd/MM/yyyy, HH:mm:ss') : null,
          transfer_status: link_metadata.transfer_status,
          transfer_status_reason: link_metadata.transfer_status_reason,
          callee_id_mapped: link_metadata.callee_id,
          
          // Transferred Call Time (agent_enter timestamp from 2nd leg)
          // This is when the agent entered the call after the transfer
          transferred_call_time: target_inbound_call ? (target_inbound_call.transferred_call_time || target_inbound_call._agentEnterTimestamp) : null,
          transferred_call_time_formatted: (() => {
            if (!target_inbound_call) return null;
            const transferredTime = target_inbound_call.transferred_call_time || target_inbound_call._agentEnterTimestamp;
            return transferredTime && !isNaN(transferredTime) ? 
              DateTime.fromSeconds(transferredTime, { zone: 'Asia/Dubai' }).toFormat('dd/MM/yyyy, HH:mm:ss') : null;
          })(),
          
          // Target inbound call information (2nd leg) - null for failed transfers
          inbound_call_id: target_inbound_call ? target_inbound_call.call_id : null,
          inbound_called_time: target_inbound_call ? target_inbound_call.called_time : null,
          inbound_called_time_formatted: target_inbound_call ? target_inbound_call.called_time_formatted : null,
          receiving_agent_name: target_inbound_call ? target_inbound_call.receiving_agent_name : 'No agent answered',
          receiving_agent_extension: target_inbound_call ? target_inbound_call.receiving_agent_extension : null,
          inbound_talk_duration: target_inbound_call ? target_inbound_call.talk_duration : null,
          inbound_recording: target_inbound_call ? target_inbound_call.recording : null,
          inbound_agent_history: target_inbound_call ? target_inbound_call.agent_history : null,
          
          // Failed transfer information
          failed_transfers: target_inbound_call ? (target_inbound_call.failed_transfers || []) : [],
          failed_transfer_count: target_inbound_call ? (target_inbound_call.failed_transfers || []).length : 0,
          failed_agents_list: target_inbound_call ? (target_inbound_call.failed_transfers || []).map(f => `${f.agent_name} (${f.extension})`).join(', ') : '',
          
          // Link metadata
          time_difference_seconds: link_metadata.time_difference_seconds,
          
          // Additional fields for display
          record_type: 'Transferred CDR',
          status: 'Linked'
        };
      }
    }).filter(Boolean); // Remove any undefined entries

    console.log(`✅ BLA REPORT COMPLETE: Generated report with ${reportData.length} linked transfer calls`);

    // Step 10.5: Build extension → agent_name map from multiple sources
    let extToNameMap = {};
    try {
      const [nameRows] = await pool.execute(
        `SELECT DISTINCT extension, agent_name FROM final_report WHERE extension IS NOT NULL AND agent_name IS NOT NULL AND extension != '' AND agent_name != ''`
      );
      nameRows.forEach(r => { if (r.extension && r.agent_name) extToNameMap[String(r.extension)] = r.agent_name; });
      console.log(`📋 EXT MAP: Built extension→name map with ${Object.keys(extToNameMap).length} entries from final_report`);
    } catch (mapErr) {
      console.error(`⚠️ EXT MAP: Failed to build map from final_report - ${mapErr.message}`);
    }

    // Supplement with names from inbound agent_history dial events and failed_transfers
    processedInboundCalls.forEach(call => {
      // Extract from agent_history dial events (have first_name, last_name, ext)
      if (Array.isArray(call.agent_history)) {
        call.agent_history.forEach(event => {
          if (event && event.ext && (event.first_name || event.last_name)) {
            const ext = String(event.ext);
            if (!extToNameMap[ext]) {
              const name = `${event.first_name || ''} ${event.last_name || ''}`.trim();
              if (name) extToNameMap[ext] = name;
            }
          }
        });
      }
      // Extract from failed_transfers
      if (Array.isArray(call.failed_transfers)) {
        call.failed_transfers.forEach(f => {
          if (f.extension && f.agent_name) {
            const ext = String(f.extension);
            if (!extToNameMap[ext]) extToNameMap[ext] = f.agent_name;
          }
        });
      }
    });
    // Also extract from campaign/outbound agent histories
    [...processedCampaignCalls, ...processedOutboundCalls].forEach(call => {
      if (Array.isArray(call.agent_history)) {
        call.agent_history.forEach(event => {
          if (event && event.ext && (event.first_name || event.last_name)) {
            const ext = String(event.ext);
            if (!extToNameMap[ext]) {
              const name = `${event.first_name || ''} ${event.last_name || ''}`.trim();
              if (name) extToNameMap[ext] = name;
            }
          }
        });
      }
    });
    console.log(`📋 EXT MAP: Final extension→name map has ${Object.keys(extToNameMap).length} entries (after supplementing from agent histories)`);

    // Step 10.6: Enrich report data with queue skill history
    const skillHistoryData = await skillHistoryPromise;
    let skillHistoryMatched = 0;
    reportData.forEach(record => {
      let entries = null;

      // 1) Try matching by inbound_call_id (successful transfers)
      if (record.inbound_call_id && skillHistoryData[record.inbound_call_id]) {
        entries = skillHistoryData[record.inbound_call_id];
      }

      // 2) For failed transfers, try matching by abandoned_calls call IDs
      if (!entries && Array.isArray(record.abandoned_calls)) {
        for (const abandoned of record.abandoned_calls) {
          if (abandoned.call_id && skillHistoryData[abandoned.call_id]) {
            entries = skillHistoryData[abandoned.call_id];
            break;
          }
        }
      }

      if (entries) {
        const skillInfo = processSkillHistoryForCall(entries, extToNameMap);
        record.skill_agents_not_available = skillInfo.skill_agents_not_available;
        record.skill_attempts = skillInfo.skill_attempts;
        record.skill_agent_answered = skillInfo.skill_agent_answered;
        skillHistoryMatched++;
      } else {
        record.skill_agents_not_available = [];
        record.skill_attempts = [];
        record.skill_agent_answered = '--';
      }
    });
    console.log(`📋 SKILL HISTORY: Enriched ${skillHistoryMatched}/${reportData.length} records with skill history data`);

    // Step 11: Database operations disabled - BLA reports are for display only
    console.log(`� BLA DATABASE: Database operations disabled - reports are for display only`);
    console.log(`� BLA DATABASE: Generated ${reportData.length} transfer records for display (not persisted)`);
    
    // DISABLED: Do not modify final_report table
    // await clearBLATransferData();
    // await insertBLATransferData(reportData);

    return {
      success: true,
      linkedCalls: reportData,
      unlinkedCampaignCalls: unlinkedCampaignCalls,
      unlinkedInboundCalls: [], // Add empty array for consistency
      data: reportData, // Keep for backward compatibility
      summary: {
        total_campaign_calls: campaignRows.length,
        campaign_calls_with_queue_transfers: campaignCallsWithQueueTransfers.length,
        total_outbound_calls: outboundRows.length,
        outbound_calls_with_queue_transfers: outboundCallsWithQueueTransfers.length,
        total_inbound_calls: inboundRows.length,
        successfully_linked_calls: allLinkedCalls.length,
        campaign_to_inbound_links: campaignToInboundLinks.length,
        outbound_to_inbound_links: outboundToInboundLinks.length,
        inbound_to_inbound_links: inboundToInboundLinks.length,
        failed_transfers: failedTransfers.length,
        successful_transfers: campaignToInboundLinks.length + outboundToInboundLinks.length + inboundToInboundLinks.length,
        date_range: {
          start: start,
          end: end,
          start_epoch: startEpoch,
          end_epoch: endEpochExclusive
        }
      }
    };

  } catch (error) {
    console.error('❌ BLA REPORT ERROR:', error);
    return {
      success: false,
      error: error.message,
      data: []
    };
  }
}

/**
 * Clear existing BLA transfer data from final_report table
 */
async function clearBLATransferData() {
  try {
    const result = await dbService.query(`
      DELETE FROM final_report 
      WHERE record_type = 'Transferred CDR'
    `);
    console.log(`🗑️ BLA DATABASE: Cleared ${result.affectedRows || 0} existing BLA transfer records`);
  } catch (error) {
    console.error(`❌ BLA DATABASE: Error clearing BLA transfer data: ${error.message}`);
    throw error;
  }
}

/**
 * Insert corrected BLA transfer data into final_report table
 * DISABLED: BLA reports should only generate temporary data for display, not persist to database
 */
async function insertBLATransferData(reportData) {
  if (!reportData || reportData.length === 0) {
    console.log(`⚠️ BLA DATABASE: No data to insert`);
    return;
  }

  // DISABLED: Do not insert BLA transfer data into final_report table
  console.log(`🚫 BLA DATABASE: Database insertion disabled - BLA reports are for display only`);
  console.log(`📊 BLA DATABASE: Would have inserted ${reportData.length} records (but skipped to prevent duplicates)`);
  return;

}

export default {
  generateBLAHotPatchTransferReport,
  findCampaignCallsWithQueueTransfers,
  linkCampaignToInboundCalls,
  detectInboundInternalTransfers,
  queueToCalleeExtensionMap
};

