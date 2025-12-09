/**
 * Enhanced CDR Matching Module
 * 
 * This module implements optimized CDR matching logic based on exact schema matching
 * from test-exact-schema-matching.js, enhanced with optimized filtering methods
 * from reportFetcher.js.
 *
 * Exports:
 * - getMatchedCDRsForFinalReport: Main function to fetch and match CDR records
 * - normalizePhoneNumber: Helper function to normalize phone numbers
 */

import dbService from './dbService.js';

/**
 * Normalize phone number by removing all non-digit characters
 * @param {string} phoneNumber - Phone number to normalize
 * @returns {string} - Normalized phone number (digits only)
 */
function normalizePhoneNumber(phoneNumber) {
  if (!phoneNumber) return '';
  return String(phoneNumber).replace(/\D/g, '');
}

/**
 * Main function to fetch and match CDR records with outbound calls
 * @param {string} tenant - Tenant identifier
 * @param {Object} params - Query parameters (startDate, endDate)
 * @returns {Promise<Array>} - Matched and enriched CDR records
 */
async function getMatchedCdrRecords(tenant, params = {}) {
  console.log('🔍 Starting enhanced CDR matching process...');
  
  try {
    // Step 1: Fetch raw CDR and outbound call records
    console.log('📊 Step 1: Fetching raw CDR and outbound call records...');
    
    const [cdrRecords, outboundRecords, cdrAllRecords] = await Promise.all([
      dbService.getRawCdrs(params),
      dbService.getRawQueueOutbound(params),
      dbService.getRawCdrsAll(params) // For follow-up notes enrichment
    ]);
    
    console.log(`📋 Fetched records: CDR: ${cdrRecords.length}, Outbound: ${outboundRecords.length}, CDR All: ${cdrAllRecords.length}`);
    
    if (cdrRecords.length === 0 || outboundRecords.length === 0) {
      console.log('⚠️ No CDR or outbound records found, returning empty result');
      return [];
    }
    
    // Step 2: Process outbound calls for matching
    console.log('🔄 Step 2: Processing outbound calls for matching...');
    const processedOutboundCalls = outboundRecords.map(outbound => {
      const rawData = outbound.raw_data ? 
        (typeof outbound.raw_data === 'string' ? JSON.parse(outbound.raw_data) : outbound.raw_data) 
        : outbound;
      
      const agentExt = rawData.agent_ext || rawData.agent_extension || rawData.Extension || outbound.agent_ext;
      const calledTime = rawData.called_time || rawData.Called_time || outbound.called_time;
      const hangupTime = rawData.hangup_time || rawData.Hangup_time || outbound.hangup_time;
      
      return {
        ...outbound,
        originalAgentExt: agentExt,
        normalizedAgentExt: normalizePhoneNumber(agentExt),
        calledTime: new Date(typeof calledTime === 'number' ? calledTime * 1000 : calledTime),
        hangupTime: new Date(typeof hangupTime === 'number' ? hangupTime * 1000 : hangupTime),
        call_id: rawData.call_id || rawData.callid || outbound.call_id || outbound.callid
      };
    }).filter(outbound => 
      outbound.normalizedAgentExt && 
      !isNaN(outbound.calledTime.getTime()) && 
      !isNaN(outbound.hangupTime.getTime())
    );
    
    console.log(`🔄 Processed ${processedOutboundCalls.length} valid outbound calls for matching`);
    
    // Step 3: Create follow-up notes lookup map from cdrs_all
    console.log('📝 Step 3: Creating follow-up notes lookup map...');
    const followUpNotesMap = new Map();
    
    cdrAllRecords.forEach((row, index) => {
      const rawData = row.raw_data || {};
      const callId = rawData.call_id;
      
      if (callId) {
        // Extract follow-up notes from various fonoUC paths
        let followUpNotes = '';
        
        if (rawData.fonoUC) {
          followUpNotes = rawData.fonoUC.cc_campaign?.follow_up_notes ||
                         rawData.fonoUC.cc?.follow_up_notes ||
                         rawData.fonoUC.cc_outbound?.follow_up_notes ||
                         rawData.fonoUC.follow_up_notes ||
                         '';
        }
        
        // Fallback to direct fields
        if (!followUpNotes) {
          followUpNotes = rawData.follow_up_notes || '';
        }
        
        if (followUpNotes) {
          followUpNotesMap.set(callId, followUpNotes);
          if (index < 3) {
            console.log(`📝 Sample follow-up note for call_id ${callId}: "${followUpNotes.substring(0, 50)}${followUpNotes.length > 50 ? '...' : ''}"`);
          }
        }
      }
    });
    
    console.log(`📝 Created follow-up notes map with ${followUpNotesMap.size} entries`);
    
    // Step 4: Apply optimized CDR matching
    console.log('🎯 Step 4: Applying optimized CDR matching...');
    const matchedCdrs = await getMatchingCdrsOptimized(cdrRecords, processedOutboundCalls);
    
    console.log(`✅ CDR matching complete: ${matchedCdrs.length} matching records found out of ${cdrRecords.length} total CDRs`);
    
    // Step 5: Enrich matched CDRs with follow-up notes and set record type
    console.log('🔄 Step 5: Enriching matched CDRs with follow-up notes...');
    const enrichedCdrs = matchedCdrs.map(cdr => {
      const rawData = cdr.raw_data || cdr;
      const callId = rawData.call_id || rawData.callid;
      
      // Create a new object for the enriched record
      const enrichedRecord = {
        ...rawData,
        _recordType: 'cdr',
        record_type: 'cdr',
        Type: 'CDR' // For frontend styling
      };
      
      // Add follow-up notes if available
      if (callId && followUpNotesMap.has(callId)) {
        const notes = followUpNotesMap.get(callId);
        enrichedRecord.follow_up_notes = notes;
        enrichedRecord['Follow up notes'] = notes; // Also add in display format
      }
      
      return enrichedRecord;
    });
    
    console.log(`✅ Enrichment complete: ${enrichedCdrs.length} CDR records enriched and ready for final report`);
    return enrichedCdrs;
    
  } catch (error) {
    console.error('❌ Error in enhanced CDR matching:', error);
    throw error;
  }
}

/**
 * Optimized CDR filtering using the best approach based on dataset size
 * @param {Array} cdrRecords - CDR records to filter
 * @param {Array} processedOutboundCalls - Processed outbound calls with normalized data
 * @returns {Promise<Array>} - Filtered CDR records
 */
async function getMatchingCdrsOptimized(cdrRecords, processedOutboundCalls) {
  if (cdrRecords.length === 0 || processedOutboundCalls.length === 0) {
    return [];
  }
  
  // For very large datasets, always use optimized in-memory approach
  // Database approach has too much overhead for large datasets
  const useDatabaseApproach = cdrRecords.length < 50000 && cdrRecords.length > 5000;

  if (useDatabaseApproach) {
    console.log('🔍 Using database-based filtering for medium-sized datasets...');
    return await getMatchingCdrsFromDatabase(cdrRecords, processedOutboundCalls);
  } else {
    console.log('💾 Using optimized in-memory filtering...');
    return await getMatchingCdrsInMemoryOptimized(cdrRecords, processedOutboundCalls);
  }
}

/**
 * Database-based CDR filtering using SQL joins
 * @param {Array} cdrRecords - CDR records to filter
 * @param {Array} processedOutboundCalls - Processed outbound calls
 * @returns {Promise<Array>} - Filtered CDR records
 */
async function getMatchingCdrsFromDatabase(cdrRecords, processedOutboundCalls) {
  console.log('🗄️ Using database-based CDR filtering with SQL joins...');
  
  // Create temporary tables for efficient joining
  const tempCdrTable = `temp_cdr_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  const tempOutboundTable = `temp_outbound_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  
  try {
    // Create temporary CDR table
    await dbService.query(`
      CREATE TEMPORARY TABLE ${tempCdrTable} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        original_index INT,
        caller_id_number VARCHAR(50),
        normalized_caller_id VARCHAR(50),
        datetime_ts BIGINT,
        record_data JSON,
        INDEX idx_caller_datetime (normalized_caller_id, datetime_ts)
      )
    `);
    
    // Create temporary outbound table
    await dbService.query(`
      CREATE TEMPORARY TABLE ${tempOutboundTable} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        agent_ext VARCHAR(50),
        normalized_agent_ext VARCHAR(50),
        called_time_ts BIGINT,
        hangup_time_ts BIGINT,
        call_data JSON,
        INDEX idx_ext_timerange (normalized_agent_ext, called_time_ts, hangup_time_ts)
      )
    `);
    
    // Prepare CDR data for batch insert
    const cdrInsertData = cdrRecords.map((cdr, index) => {
      const rawData = cdr.raw_data || cdr;
      const callerIdNumber = rawData.caller_id_number || rawData.Caller_ID_Number || cdr.caller_id_number;
      const dateTime = rawData.datetime || rawData.timestamp || rawData.called_time || cdr.datetime || cdr.timestamp || cdr.called_time;
      
      // Handle Gregorian timestamp conversion for all CDR record types
      let convertedDateTime = dateTime;
      if (typeof dateTime === 'number' && dateTime > 60000000000) {
        // Gregorian timestamp: subtract 62167219200 seconds (correct offset from 0001-01-01 to 1970-01-01)
        const unixSeconds = dateTime - 62167219200;
        convertedDateTime = unixSeconds * 1000; // Convert to milliseconds
      } else if (typeof dateTime === 'number' && dateTime < 10000000000) {
        // Handle Unix timestamp in seconds (convert to milliseconds)
        convertedDateTime = dateTime * 1000;
      }
      
      const normalizedCallerId = normalizePhoneNumber(callerIdNumber);
      const datetimeTs = new Date(convertedDateTime).getTime();
      
      return [
        index,
        callerIdNumber || '',
        normalizedCallerId || '',
        isNaN(datetimeTs) ? 0 : datetimeTs,
        JSON.stringify(cdr)
      ];
    }).filter(row => row[2] && row[3] > 0); // Filter out invalid records
    
    // Prepare outbound data for batch insert
    const outboundInsertData = processedOutboundCalls.map(outbound => [
      outbound.originalAgentExt || '',
      outbound.normalizedAgentExt || '',
      outbound.calledTime.getTime(),
      outbound.hangupTime.getTime(),
      JSON.stringify(outbound)
    ]).filter(row => row[1] && row[2] > 0 && row[3] > 0);
    
    console.log(`📊 Preparing to insert ${cdrInsertData.length} CDRs and ${outboundInsertData.length} outbound calls into temp tables...`);
    
    // Batch insert CDR data in chunks to avoid placeholder limit
    if (cdrInsertData.length > 0) {
      const BATCH_SIZE = 1000; // Safe batch size to avoid placeholder limit
      for (let i = 0; i < cdrInsertData.length; i += BATCH_SIZE) {
        const batch = cdrInsertData.slice(i, i + BATCH_SIZE);
        const placeholders = batch.map(() => '(?, ?, ?, ?, ?)').join(', ');
        const flatParams = batch.flat();
        await dbService.query(
          `INSERT INTO ${tempCdrTable} (original_index, caller_id_number, normalized_caller_id, datetime_ts, record_data) VALUES ${placeholders}`,
          flatParams
        );
      }
      console.log(`📊 Inserted ${cdrInsertData.length} CDR records in ${Math.ceil(cdrInsertData.length / BATCH_SIZE)} batches`);
    }
    
    // Batch insert outbound data in chunks to avoid placeholder limit
    if (outboundInsertData.length > 0) {
      const BATCH_SIZE = 1000; // Safe batch size to avoid placeholder limit
      for (let i = 0; i < outboundInsertData.length; i += BATCH_SIZE) {
        const batch = outboundInsertData.slice(i, i + BATCH_SIZE);
        const placeholders = batch.map(() => '(?, ?, ?, ?, ?)').join(', ');
        const flatParams = batch.flat();
        await dbService.query(
          `INSERT INTO ${tempOutboundTable} (agent_ext, normalized_agent_ext, called_time_ts, hangup_time_ts, call_data) VALUES ${placeholders}`,
          flatParams
        );
      }
      console.log(`📊 Inserted ${outboundInsertData.length} outbound records in ${Math.ceil(outboundInsertData.length / BATCH_SIZE)} batches`);
    }
    
    // Execute optimized join query
    const joinQuery = `
      SELECT DISTINCT c.original_index, c.record_data
      FROM ${tempCdrTable} c
      INNER JOIN ${tempOutboundTable} o ON (
        c.normalized_caller_id = o.normalized_agent_ext
        AND c.datetime_ts >= o.called_time_ts
        AND c.datetime_ts <= o.hangup_time_ts
      )
      ORDER BY c.original_index
    `;
    
    console.log('🔍 Executing optimized join query...');
    const results = await dbService.query(joinQuery);
    
    // Extract matched CDR records in original order
    const matchedCdrs = results.map(row => {
      try {
        return typeof row.record_data === 'string' ? JSON.parse(row.record_data) : row.record_data;
      } catch (error) {
        console.error('Error parsing record_data:', error, 'Raw data:', row.record_data);
        return row.record_data; // Return as-is if parsing fails
      }
    });
    
    console.log(`✅ Database join completed: ${matchedCdrs.length} matching CDRs found`);
    return matchedCdrs;
    
  } finally {
    // Clean up temporary tables
    try {
      await dbService.query(`DROP TEMPORARY TABLE IF EXISTS ${tempCdrTable}`);
      await dbService.query(`DROP TEMPORARY TABLE IF EXISTS ${tempOutboundTable}`);
    } catch (cleanupError) {
      console.warn('⚠️ Error cleaning up temporary tables:', cleanupError.message);
    }
  }
}

/**
 * Optimized in-memory CDR filtering using efficient lookup structures
 * @param {Array} cdrRecords - CDR records to filter
 * @param {Array} processedOutboundCalls - Processed outbound calls
 * @returns {Promise<Array>} - Filtered CDR records
 */
async function getMatchingCdrsInMemoryOptimized(cdrRecords, processedOutboundCalls) {
  console.log('💾 Using optimized in-memory CDR filtering...');
  console.log(`⚠️ DIAGNOSTIC: Filtering ${cdrRecords.length} CDR records against ${processedOutboundCalls.length} outbound calls`);
  
  // Create efficient lookup structures
  const outboundByExt = new Map();
  
  // Group outbound calls by normalized agent extension for O(1) lookup
  processedOutboundCalls.forEach(outbound => {
    const ext = outbound.normalizedAgentExt;
    if (!outboundByExt.has(ext)) {
      outboundByExt.set(ext, []);
    }
    outboundByExt.get(ext).push(outbound);
  });
  
  console.log(`📊 Created lookup map with ${outboundByExt.size} unique extensions`);
  
  // DIAGNOSTIC: Log some sample extensions to help with debugging
  if (outboundByExt.size > 0) {
    console.log('⚠️ DIAGNOSTIC: Sample extensions in lookup map:');
    let count = 0;
    for (const [ext, calls] of outboundByExt.entries()) {
      if (count < 5) {
        console.log(`   - Extension: ${ext}, Calls: ${calls.length}`);
        count++;
      } else {
        break;
      }
    }
  } else {
    console.log('⚠️ DIAGNOSTIC: No extensions in lookup map - this will result in no CDR matches');
  }
  
  const matchedCdrs = [];
  let processedCount = 0;
  
  for (const cdr of cdrRecords) {
    processedCount++;
    
    // Extract data from raw_data JSON or direct fields
    const rawData = cdr.raw_data || cdr;
    const callerIdNumber = rawData.caller_id_number || rawData.Caller_ID_Number || cdr.caller_id_number;
    const dateTime = rawData.datetime || rawData.timestamp || rawData.called_time || cdr.datetime || cdr.timestamp || cdr.called_time;
    
    const cdrCallerIdNumber = normalizePhoneNumber(callerIdNumber);
    
    // Handle Gregorian timestamp conversion for all CDR record types
    let convertedDateTime = dateTime;
    if (typeof dateTime === 'number' && dateTime > 60000000000) {
      const unixSeconds = dateTime - 62167219200;
      convertedDateTime = unixSeconds * 1000;
    } else if (typeof dateTime === 'number' && dateTime < 10000000000) {
      // Handle Unix timestamp in seconds (convert to milliseconds)
      convertedDateTime = dateTime * 1000;
    }
    
    const cdrDateTime = new Date(convertedDateTime);
    
    // Skip invalid records
    if (!cdrCallerIdNumber || isNaN(cdrDateTime.getTime())) {
      continue;
    }
    
    // Quick lookup for matching extensions
    const matchingOutbounds = outboundByExt.get(cdrCallerIdNumber);
    if (!matchingOutbounds) {
      continue; // No outbound calls for this extension
    }
    
    // Check time overlap with matching extensions
    const matchFound = matchingOutbounds.some(outbound => {
      return cdrDateTime >= outbound.calledTime && cdrDateTime <= outbound.hangupTime;
    });
    
    if (matchFound) {
      matchedCdrs.push(cdr);
      
      if (processedCount <= 3) {
        const matchingOutbound = matchingOutbounds.find(outbound => 
          cdrDateTime >= outbound.calledTime && cdrDateTime <= outbound.hangupTime
        );
        const cdrCallId = rawData.custom_channel_vars?.bridge_id || rawData.call_id || cdr.call_id || 'unknown';
        const outboundCallId = matchingOutbound.call_id || matchingOutbound.callid || 'unknown';
        console.log(`✅ CDR ${cdrCallId} matches outbound ${outboundCallId} (ext: ${cdrCallerIdNumber})`);
      }
    }
    
    // Progress logging for large datasets
    if (processedCount % 1000 === 0) {
      console.log(`📊 Processed ${processedCount}/${cdrRecords.length} CDRs, found ${matchedCdrs.length} matches`);
    }
  }
  
  console.log(`✅ In-memory matching completed: ${matchedCdrs.length} matching CDRs found`);
  return matchedCdrs;
}

/**
 * Fallback CDR filtering using original nested loop approach
 * @param {Array} cdrRecords - CDR records to filter
 * @param {Array} processedOutboundCalls - Processed outbound calls
 * @returns {Promise<Array>} - Filtered CDR records
 */
async function fallbackCdrFiltering(cdrRecords, processedOutboundCalls) {
  console.log('🔄 Using fallback filtering method...');
  
  const filteredCDRs = cdrRecords.filter((cdr, index) => {
    const rawData = cdr.raw_data || cdr;
    const callerIdNumber = rawData.caller_id_number || rawData.Caller_ID_Number || cdr.caller_id_number;
    const dateTime = rawData.datetime || rawData.timestamp || rawData.called_time || cdr.datetime || cdr.timestamp || cdr.called_time;
    
    const cdrCallerIdNumber = normalizePhoneNumber(callerIdNumber);
    
    let convertedDateTime = dateTime;
    if (typeof dateTime === 'number' && dateTime > 60000000000) {
      const unixSeconds = dateTime - 62167219200;
      convertedDateTime = unixSeconds * 1000;
    } else if (typeof dateTime === 'number' && dateTime < 10000000000) {
      // Handle Unix timestamp in seconds (convert to milliseconds)
      convertedDateTime = dateTime * 1000;
    }
    
    const cdrDateTime = new Date(convertedDateTime);
    
    if (!cdrCallerIdNumber || isNaN(cdrDateTime.getTime())) {
      return false;
    }
    
    return processedOutboundCalls.some(outboundCall => {
      const extensionMatch = cdrCallerIdNumber === outboundCall.normalizedAgentExt;
      const timeMatch = cdrDateTime >= outboundCall.calledTime && cdrDateTime <= outboundCall.hangupTime;
      return extensionMatch && timeMatch;
    });
  });
  
  return filteredCDRs;
}

export {
  getMatchedCdrRecords as getMatchedCDRsForFinalReport,
  normalizePhoneNumber
};
