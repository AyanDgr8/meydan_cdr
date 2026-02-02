// dbService.js
// Comprehensive database service for SPC CDR reporting system

import mysql from 'mysql2/promise';
import dotenv from 'dotenv';

dotenv.config();

// Create a connection pool

// const pool = mysql.createPool({
//    host:"0.0.0.0",
//    user: 'multycomm',
//    password: 'WELcome@123',
//    database: 'meydan_main_cdr',
//    port: 3306,
//    waitForConnections: true,
//    connectionLimit: 50,  // Increased for better multi-tab performance
//    queueLimit: 25,       // Added queue limit to prevent overwhelming the server
//    multipleStatements: true,
//    connectTimeout: 60000,  // 60 seconds connection timeout
//    acquireTimeout: 60000,  // 60 seconds acquire timeout
//    timeout: 180000,       // 180 seconds query timeout
//    enableKeepAlive: true, // Enable connection keep-alive
//    keepAliveInitialDelay: 10000 // Keep-alive ping every 10 seconds
//  });

const pool = mysql.createPool({
   host:"localhost",
   user: 'root',
   password: 'Ayan@1012',
   database: 'meydan_main_cdr',
   port: 3306,
   waitForConnections: true,
   connectionLimit: 50,  // Increased for better multi-tab performance
   queueLimit: 25,       // Added queue limit to prevent overwhelming the server
   multipleStatements: true,
   connectTimeout: 60000,  // 60 seconds connection timeout
   acquireTimeout: 60000,  // 60 seconds acquire timeout
   timeout: 180000,       // 180 seconds query timeout
   enableKeepAlive: true, // Enable connection keep-alive
   keepAliveInitialDelay: 10000 // Keep-alive ping every 10 seconds
 });


/**
 * Normalize a phone number for consistent comparison
 * @param {string} phoneNumber - Phone number to normalize
 * @returns {string} - Normalized phone number (digits only, last 10 digits)
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
  if (normalized.length > 10) {
    normalized = normalized.slice(-10);
  }
  
  return normalized;
}

// Utility function to convert timestamps to Unix seconds for database queries
function convertTimestamp(timestamp) {
  if (timestamp === null || timestamp === undefined) {
    return null;
  }

  // If it's already a Date object
  if (timestamp instanceof Date) {
    return Math.floor(timestamp.getTime() / 1000); // Return Unix seconds
  }

  // If it's a number (seconds or milliseconds)
  if (typeof timestamp === 'number') {
    // If it's already in seconds, return as-is; if milliseconds, convert to seconds
    return timestamp < 10000000000 ? timestamp : Math.floor(timestamp / 1000);
  }

  // If it's a string, try to parse it
  if (typeof timestamp === 'string') {
    const parsed = Date.parse(timestamp);
    if (!isNaN(parsed)) {
      return Math.floor(parsed / 1000); // Return Unix seconds
    }
  }

  return null; // Return null for invalid timestamps
}

/**
 * Helper function to normalize date parameters to support both naming conventions
 * @param {Object} filters - Filter object containing date parameters
 * @returns {Object} - Object with normalized startDate and endDate properties
 */
function normalizeDateParams(filters = {}) {
  return {
    startDate: filters.start_date || filters.startDate || null,
    endDate: filters.end_date || filters.endDate || null
  };
}

// Extract raw timestamp value for BIGINT columns
function extractRawTimestamp(timestamp) {
  if (!timestamp) return null;
  
  let result = null;
  const originalValue = timestamp;
  
  if (typeof timestamp === 'number') {
    // For BIGINT columns, we want the raw Unix timestamp in seconds
    result = timestamp > 1e10 ? Math.floor(timestamp / 1000) : timestamp;
  } else if (typeof timestamp === 'string') {
    // Try to convert string to timestamp
    const date = new Date(timestamp);
    result = isNaN(date.getTime()) ? null : Math.floor(date.getTime() / 1000);
  }
  
  // // Add debug logging for timestamp conversion
  // console.log(`Timestamp conversion: ${typeof originalValue} ${originalValue} -> ${result}`);
  
  return result;
}

// // Extract CDRs All timestamp value using the same logic as reportFetcher.js
// function extractCdrsAllTimestamp(timestamp) {
//   if (!timestamp) return null;
  
//   try {
//     let ts = timestamp;
//     if (typeof timestamp === 'string') {
//       ts = parseFloat(timestamp);
//       if (isNaN(ts)) return null;
//     }
    
//     if (typeof ts !== 'number' || isNaN(ts)) return null;
    
//     // Handle Gregorian timestamps (convert to Unix timestamp)
//     if (ts > 60000000000) {
//       const unixSeconds = ts - 62167219200;
//       return unixSeconds; // Return Unix seconds
//     } else if (ts < 10_000_000_000) {
//       // Already in Unix seconds
//       return ts;
//     } else {
//       // Unix milliseconds, convert to seconds
//       return Math.floor(ts / 1000);
//     }
//   } catch (error) {
//     console.warn('Error extracting CDRs All timestamp:', timestamp, error);
//     return null;
//   }
// }

/**
 * Execute a query with parameters
 * @param {string} sql - SQL query
 * @param {Array} params - Query parameters
 * @returns {Promise<Array>} - Query results
 */
async function query(sql, params) {
  const startTime = Date.now();
  let connection;
  let retryCount = 0;
  const maxRetries = 3;
  const initialBackoff = 100; // Start with 100ms backoff
  
  while (retryCount <= maxRetries) {
    try {
      // Get a connection from the pool with timeout protection
      const connectionPromise = pool.getConnection();
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error('Connection acquisition timeout')), 10000); // 10 second timeout
      });
      
      try {
        connection = await Promise.race([connectionPromise, timeoutPromise]);
      } catch (connError) {
        if (connError.message === 'Connection acquisition timeout') {
          console.warn(`⚠️ Connection acquisition timeout, retry ${retryCount + 1}/${maxRetries}`);
          retryCount++;
          if (retryCount <= maxRetries) {
            await new Promise(resolve => setTimeout(resolve, initialBackoff * Math.pow(2, retryCount)));
            continue;
          }
        }
        throw connError;
      }
      
      // Execute the query with timeout protection
      const [rows] = await connection.execute(sql, params);
      
      const duration = Date.now() - startTime;
      if (duration > 1000) { // Only log slow queries (>1s)
        console.log(`✅ Query executed in ${duration}ms, returned ${rows?.length || 0} rows`);
      }
      
      return rows;
    } catch (error) {
      const duration = Date.now() - startTime;
      
      // Check if error is retryable
      const isRetryable = 
        error.code === 'ETIMEDOUT' || 
        error.code === 'ECONNRESET' || 
        error.code === 'ER_LOCK_WAIT_TIMEOUT' ||
        error.message.includes('too many connections');
      
      if (isRetryable && retryCount < maxRetries) {
        retryCount++;
        const backoffTime = initialBackoff * Math.pow(2, retryCount);
        console.warn(`⚠️ Retryable database error (${error.code}), attempt ${retryCount}/${maxRetries} after ${backoffTime}ms delay`);
        await new Promise(resolve => setTimeout(resolve, backoffTime));
        continue;
      }
      
      // Categorize and log the error
      if (error.code === 'ETIMEDOUT' || error.code === 'ECONNRESET') {
        console.error(`❌ Database connection error (${error.code}) after ${duration}ms:`, error.message);
      } else if (error.code === 'ER_LOCK_WAIT_TIMEOUT') {
        console.error(`❌ Database lock timeout after ${duration}ms:`, error.message);
      } else {
        console.error(`❌ Database query error after ${duration}ms:`, error.message);
      }
      
      // Enhance the error with more context
      error.queryDuration = duration;
      error.sql = sql.substring(0, 200) + (sql.length > 200 ? '...' : '');
      throw error;
    } finally {
      // Always release the connection back to the pool
      if (connection) {
        connection.release();
      }
    }
  }
}

/**
 * Execute optimized bulk inserts using VALUES clause for maximum performance
 * @param {string} sql - Base SQL query (INSERT INTO table (columns))
 * @param {Array<Array>} batchParams - Array of parameter arrays for each record
 * @param {number} chunkSize - Number of records per bulk insert (default: 1000)
 * @returns {Promise<Object>} - Result object
 */
async function batchInsert(sql, batchParams, chunkSize = 1000) {
  if (!batchParams || batchParams.length === 0) {
    return { affectedRows: 0 };
  }
  
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    
    let totalAffected = 0;
    
    // Process in chunks for optimal performance
    for (let i = 0; i < batchParams.length; i += chunkSize) {
      const chunk = batchParams.slice(i, i + chunkSize);
      
      // Build bulk VALUES clause
      const placeholders = chunk.map(() => `(${chunk[0].map(() => '?').join(', ')})`).join(', ');
      const bulkSql = `${sql} VALUES ${placeholders}`;
      
      // Flatten parameters for bulk insert
      const flatParams = chunk.flat();
      
      const [result] = await connection.execute(bulkSql, flatParams);
      totalAffected += result.affectedRows;
    }
    
    await connection.commit();
    return { affectedRows: totalAffected };
  } catch (error) {
    await connection.rollback();
    console.error('Bulk insert error:', error);
    throw error;
  } finally {
    connection.release();
  }
}

/**
 * Begin a transaction
 * @returns {Promise<mysql.Connection>} - Connection with active transaction
 */
export async function beginTransaction() {
  const connection = await pool.getConnection();
  await connection.beginTransaction();
  return connection;
}

/**
 * Commit a transaction
 * @param {mysql.Connection} connection - Connection with active transaction
 */
export async function commitTransaction(connection) {
  try {
    await connection.commit();
  } finally {
    connection.release();
  }
}

/**
 * Rollback a transaction
 * @param {mysql.Connection} connection - Connection with active transaction
 */
export async function rollbackTransaction(connection) {
  try {
    await connection.rollback();
  } finally {
    connection.release();
  }
}


/**
 * Insert raw campaign data into raw_campaigns table
 * @param {Object} data - Raw campaign data from API
 * @returns {Promise<number>} - Inserted ID
 */
export async function insertRawCampaigns(data) {
  const sql = `INSERT IGNORE INTO raw_campaigns (
    call_id, campaign_name, timestamp, raw_data
  ) VALUES (?, ?, ?, ?)`;

  const callId = data.call_id || data.callid || null;
  const campaignName = data.campaign_name || null;
  const timestamp = extractRawTimestamp(data.event_timestamp || data.timestamp || data.called_time) || null;
  const rawData = JSON.stringify(data);

  const params = [callId, campaignName, timestamp, rawData];

  return await query(sql, params);
}

/**
 * Batch insert raw campaign data into raw_campaigns table
 * @param {Array<Object>} campaignsData - Array of raw campaign data objects
 * @returns {Promise<number>} - Number of affected rows
 */
export async function batchInsertRawCampaigns(campaignsData) {
  if (!campaignsData || campaignsData.length === 0) {
    return { affectedRows: 0 };
  }
  
  const sql = `INSERT IGNORE INTO raw_campaigns (
    call_id, campaign_name, timestamp, raw_data
  ) VALUES (?, ?, ?, ?)`;
  
  // Prepare batch parameters with simplified schema fields
  const batchParams = campaignsData.map(campaignData => {
    const callId = campaignData.call_id || campaignData.callid || null;
    const campaignName = campaignData.campaign_name || null;
    const timestamp = extractRawTimestamp(campaignData.event_timestamp || campaignData.timestamp || campaignData.called_time) || null;
    const rawData = JSON.stringify(campaignData);
    
    return [callId, campaignName, timestamp, rawData];
  });
  
  // Use optimized bulk insert with simplified schema
  const baseSql = `INSERT IGNORE INTO raw_campaigns (
    call_id, campaign_name, timestamp, raw_data
  )`;
  
  try {
    const result = await batchInsert(baseSql, batchParams);
    console.log(`Bulk inserted ${result.affectedRows} campaign records`);
    return result.affectedRows;
  } catch (error) {
    console.error('Error batch inserting raw campaign data:', error);
    throw error;
  }
}

/**
 * Insert raw inbound queue data into raw_queue_inbound table
 * @param {Object} queueData - Raw inbound queue data from API
 * @returns {Promise<number>} - Inserted ID
 */
export async function insertRawQueueInbound(queueData) {
  const sql = `INSERT IGNORE INTO raw_queue_inbound (
    callid, queue_name, called_time, raw_data
  ) VALUES (?, ?, ?, ?)`;
  
  const callId = queueData.call_id || queueData.callid || null;
  const queueName = queueData.queue_name || null;
  const calledTime = extractRawTimestamp(queueData.called_time) || null;
  const rawData = JSON.stringify(queueData);
  
  const params = [callId, queueName, calledTime, rawData];
  
  try {
    const result = await query(sql, params);
    return result.insertId;
  } catch (error) {
    console.error('Error inserting raw inbound queue data:', error);
    throw error;
  }
}

/**
 * Batch insert raw inbound queue data into raw_queue_inbound table
 * @param {Array<Object>} queueDataArray - Array of raw inbound queue data objects
 * @returns {Promise<number>} - Number of affected rows
 */
export async function batchInsertRawQueueInbound(queueDataArray) {
  if (!queueDataArray || queueDataArray.length === 0) {
    return { affectedRows: 0 };
  }
  
  const sql = `INSERT IGNORE INTO raw_queue_inbound (
    callid, queue_name, called_time, raw_data
  ) VALUES (?, ?, ?, ?)`;
  
  // Prepare batch parameters
  const batchParams = queueDataArray.map(queueData => {
    const callId = queueData.call_id || queueData.callid || null;
    const queueName = queueData.queue_name || null;
    const calledTime = extractRawTimestamp(queueData.called_time) || null;
    const rawData = JSON.stringify(queueData);
    
    return [callId, queueName, calledTime, rawData];
  });
  
  // Use optimized bulk insert with base SQL
  const baseSql = `INSERT IGNORE INTO raw_queue_inbound (
    callid, queue_name, called_time, raw_data
  )`;
  
  try {
    const result = await batchInsert(baseSql, batchParams);
    console.log(`Bulk inserted ${result.affectedRows} inbound queue records`);
    return result.affectedRows;
  } catch (error) {
    console.error('Error batch inserting raw inbound queue data:', error);
    throw error;
  }
}

/**
 * Insert raw outbound queue data into raw_queue_outbound table
 * @param {Object} outboundData - Raw outbound queue data from API
 * @returns {Promise<number>} - Inserted ID
 */
export async function insertRawQueueOutbound(outboundData) {
  const sql = `INSERT IGNORE INTO raw_queue_outbound (
    callid, queue_name, called_time, raw_data
  ) VALUES (?, ?, ?, ?)`;
  
  // Extract key fields for indexing
  // Map call_id to callid (schema column name)
  const callId = outboundData.call_id || outboundData.callid || null;
  const queueName = outboundData.queue_name || null;
  const calledTime = extractRawTimestamp(outboundData.called_time) || null;
  
  // Store the entire raw data as JSON
  const rawData = JSON.stringify(outboundData);
  
  const params = [callId, queueName, calledTime, rawData];
  
  try {
    const result = await query(sql, params);
    return result.insertId;
  } catch (error) {
    console.error('Error inserting raw outbound queue data:', error);
    throw error;
  }
}

/**
 * Batch insert raw outbound queue data into raw_queue_outbound table
 * @param {Array<Object>} outboundDataArray - Array of raw outbound queue data objects
 * @returns {Promise<number>} - Number of affected rows
 */
export async function batchInsertRawQueueOutbound(outboundDataArray) {
  if (!outboundDataArray || outboundDataArray.length === 0) {
    return { affectedRows: 0 };
  }
  
  // Prepare batch parameters
  const batchParams = outboundDataArray.map(outboundData => {
    const callId = outboundData.call_id || outboundData.callid || null;
    const queueName = outboundData.queue_name || null;
    const calledTime = extractRawTimestamp(outboundData.called_time) || null;
    const rawData = JSON.stringify(outboundData);
    
    return [callId, queueName, calledTime, rawData];
  });

  // Use optimized bulk insert with base SQL
  const baseSql = `INSERT IGNORE INTO raw_queue_outbound (
    callid, queue_name, called_time, raw_data
  )`;

  try {
    const result = await batchInsert(baseSql, batchParams);
    console.log(`Bulk inserted ${result.affectedRows} outbound queue records`);
    return result.affectedRows;
  } catch (error) {
    console.error('Error batch inserting raw outbound queue data:', error);
    throw error;
  }
}

// /**
//  * Batch insert raw CDRs All data into raw_cdrs_all table
//  * @param {Array<Object>} cdrsAllDataArray - Array of raw CDRs All data objects
//  * @returns {Promise<number>} - Number of affected rows
//  */
// export async function batchInsertRawCdrsAll(cdrsAllDataArray) {
//   if (!cdrsAllDataArray || cdrsAllDataArray.length === 0) {
//     return { affectedRows: 0 };
//   }
  
//   // Prepare batch parameters
//   const batchParams = cdrsAllDataArray.map(cdrsAllData => {
//     const callId = cdrsAllData.call_id || null;
//     const timestamp = extractCdrsAllTimestamp(cdrsAllData.timestamp) || null;
//     const rawData = JSON.stringify(cdrsAllData);
    
//     return [callId, timestamp, rawData];
//   });

//   // Use optimized bulk insert with base SQL
//   const baseSql = `INSERT IGNORE INTO raw_cdrs_all (
//     call_id, timestamp, raw_data
//   )`;

//   try {
//     const result = await batchInsert(baseSql, batchParams);
//     console.log(`Bulk inserted ${result.affectedRows} CDRs All records`);
//     return result.affectedRows;
//   } catch (error) {
//     console.error('Error batch inserting raw CDRs All data:', error);
//     throw error;
//   }
// }

// /**
//  * Retrieve raw CDRs All data from database
//  * @param {Object} filters - Filter criteria
//  * @returns {Promise<Array>} - Array of raw CDRs All records
//  */
// export async function getRawCdrsAll(filters = {}) {
//   let sql = 'SELECT * FROM raw_cdrs_all WHERE 1=1';
//   const params = [];
  
//   // Normalize date parameters to support both naming conventions
//   const { startDate, endDate } = normalizeDateParams(filters);
  
//   if (startDate) {
//     const startMs = convertTimestamp(startDate);
//     sql += ' AND timestamp >= ?';
//     params.push(startMs);
//   }
  
//   if (endDate) {
//     const endMs = convertTimestamp(endDate);
//     sql += ' AND timestamp <= ?';
//     params.push(endMs);
//   }
  
//   if (filters.callId) {
//     sql += ' AND call_id = ?';
//     params.push(filters.callId);
//   }
  
//   sql += ' ORDER BY timestamp ASC';
  
//   try {
//     const rows = await query(sql, params);
//     return rows.map(row => ({
//       ...row,
//       raw_data: typeof row.raw_data === 'string' ? JSON.parse(row.raw_data) : row.raw_data
//     }));
//   } catch (error) {
//     console.error('Error retrieving raw CDRs All:', error);
//     throw error;
//   }
// }

// /**
//  * Process CDRs All data to extract recordings and insert into recordings table
//  * @param {Object} filters - Filter criteria for date range
//  * @returns {Promise<Object>} - Result object with processed recordings count
//  */
// export async function processRecordingsFromCdrsAll(filters = {}) {
//   console.log('🎵 Processing recordings from CDRs All data...');
  
//   try {
//     // Get CDRs All data from database
//     const cdrsAllData = await getRawCdrsAll(filters);
//     console.log(`📊 Found ${cdrsAllData.length} CDRs All records to process`);
    
//     const recordingsToInsert = [];
    
//     // Process each CDR record to extract recordings
//     for (const record of cdrsAllData) {
//       const rawData = record.raw_data;
//       const mediaRecordings = rawData.custom_channel_vars?.media_recordings;
      
//       if (mediaRecordings) {
//         let recordingIds = '';
        
//         if (Array.isArray(mediaRecordings) && mediaRecordings.length > 0) {
//           // Multiple recordings - join them with comma
//           recordingIds = mediaRecordings.filter(id => id && id.trim() !== '').join(',');
//         } else if (typeof mediaRecordings === 'string' && mediaRecordings.trim() !== '') {
//           // Single recording as string
//           recordingIds = mediaRecordings.trim();
//         }
        
//         if (recordingIds) {
//           const convertedTimestamp = extractCdrsAllTimestamp(rawData.timestamp);
//           recordingsToInsert.push({
//             call_id: rawData.call_id,
//             called_time: convertedTimestamp,
//             called_time_formatted: convertedTimestamp ? new Date(convertedTimestamp * 1000).toLocaleString() : '',
//             caller_id_number: rawData.caller_id_number || '',
//             caller_id_name: rawData.caller_id_name || '',
//             callee_id_number: rawData.callee_id_number || '',
//             callee_id_name: rawData.callee_id_name || '',
//             recording_id: recordingIds
//           });
//         }
//       }
//     }
    
//     console.log(`🎵 Found ${recordingsToInsert.length} recordings to insert`);
    
//     if (recordingsToInsert.length > 0) {
//       // Batch insert recordings
//       const result = await batchInsertRecordings(recordingsToInsert);
//       console.log(`✅ Processed ${result.affectedRows} recordings successfully`);
//       return { success: true, recordingsProcessed: result.affectedRows };
//     } else {
//       console.log('ℹ️ No recordings found to process');
//       return { success: true, recordingsProcessed: 0 };
//     }
    
//   } catch (error) {
//     console.error('❌ Error processing recordings from CDRs All:', error);
//     throw error;
//   }
// }

// /**
//  * Batch insert recordings into recordings table
//  * @param {Array<Object>} recordingsArray - Array of recording objects
//  * @returns {Promise<number>} - Number of affected rows
//  */
// export async function batchInsertRecordings(recordingsArray) {
//   if (!recordingsArray || recordingsArray.length === 0) {
//     return { affectedRows: 0 };
//   }
  
//   // Prepare batch parameters
//   const batchParams = recordingsArray.map(recording => [
//     recording.call_id,
//     recording.called_time,
//     recording.called_time_formatted,
//     recording.caller_id_number,
//     recording.caller_id_name,
//     recording.callee_id_number,
//     recording.callee_id_name,
//     recording.recording_id
//   ]);

//   // Use optimized bulk insert with base SQL
//   const baseSql = `INSERT IGNORE INTO recordings (
//     call_id, called_time, called_time_formatted, caller_id_number, caller_id_name, 
//     callee_id_number, callee_id_name, recording_id
//   )`;

//   try {
//     const result = await batchInsert(baseSql, batchParams);
//     console.log(`Bulk inserted ${result.affectedRows} recording records`);
//     return result.affectedRows;
//   } catch (error) {
//     console.error('Error batch inserting recordings:', error);
//     throw error;
//   }
// }

// /**
//  * Get recordings data from database with filtering
//  * @param {Object} filters - Filter criteria
//  * @returns {Promise<Array>} - Array of recording records
//  */
// export async function getRecordings(filters = {}) {
//   let sql = 'SELECT * FROM recordings WHERE 1=1';
//   const params = [];
  
//   // Normalize date parameters to support both naming conventions
//   const { startDate, endDate } = normalizeDateParams(filters);
  
//   if (startDate) {
//     const startMs = convertTimestamp(startDate);
//     sql += ' AND called_time >= ?';
//     params.push(startMs);
//   }
  
//   if (endDate) {
//     const endMs = convertTimestamp(endDate);
//     sql += ' AND called_time <= ?';
//     params.push(endMs);
//   }
  
//   if (filters.callId) {
//     sql += ' AND call_id = ?';
//     params.push(filters.callId);
//   }
  
//   if (filters.callerIdNumber) {
//     sql += ' AND caller_id_number LIKE ?';
//     params.push(`%${filters.callerIdNumber}%`);
//   }
  
//   if (filters.calleeIdNumber) {
//     sql += ' AND callee_id_number LIKE ?';
//     params.push(`%${filters.calleeIdNumber}%`);
//   }
  
//   sql += ' ORDER BY called_time DESC';
  
//   try {
//     const rows = await query(sql, params);
//     console.log(`📊 Retrieved ${rows.length} recording records`);
//     return rows;
//   } catch (error) {
//     console.error('Error retrieving recordings:', error);
//     throw error;
//   }
// }


/**
 * Retrieve raw campaigns data from database
 * @param {Object} filters - Filter criteria (startDate, endDate, etc.)
 * @returns {Promise<Array>} - Array of raw campaign records
 */
export async function getRawCampaigns(filters = {}) {
  let sql = 'SELECT * FROM raw_campaigns WHERE 1=1';
  const params = [];
  
  // Normalize date parameters to support both naming conventions
  const { startDate, endDate } = normalizeDateParams(filters);
  
  if (startDate) {
    const startMs = convertTimestamp(startDate);
    sql += ' AND timestamp >= ?';
    params.push(startMs);
  }
  
  if (endDate) {
    const endMs = convertTimestamp(endDate);
    sql += ' AND timestamp <= ?';
    params.push(endMs);
  }
  
  if (filters.campaignName) {
    sql += ' AND campaign_name = ?';
    params.push(filters.campaignName);
  }
  
  sql += ' ORDER BY timestamp ASC';
  
  try {
    const rows = await query(sql, params);
    return rows.map(row => ({
      ...row,
      raw_data: typeof row.raw_data === 'string' ? JSON.parse(row.raw_data) : row.raw_data
    }));
  } catch (error) {
    console.error('Error retrieving raw campaigns:', error);
    throw error;
  }
}

/**
 * Retrieve raw queue inbound data from database
 * @param {Object} filters - Filter criteria
 * @returns {Promise<Array>} - Array of raw queue inbound records
 */
export async function getRawQueueInbound(filters = {}) {
  let sql = 'SELECT * FROM raw_queue_inbound WHERE 1=1';
  const params = [];
  
  // Normalize date parameters to support both naming conventions
  const { startDate, endDate } = normalizeDateParams(filters);
  
  if (startDate) {
    const startMs = convertTimestamp(startDate);
    sql += ' AND called_time >= ?';
    params.push(startMs);
  }
  
  if (endDate) {
    const endMs = convertTimestamp(endDate);
    sql += ' AND called_time <= ?';
    params.push(endMs);
  }
  
  if (filters.queueName) {
    sql += ' AND queue_name = ?';
    params.push(filters.queueName);
  }
  
  sql += ' ORDER BY called_time ASC';
  
  try {
    const rows = await query(sql, params);
    return rows.map(row => ({
      ...row,
      raw_data: typeof row.raw_data === 'string' ? JSON.parse(row.raw_data) : row.raw_data
    }));
  } catch (error) {
    console.error('Error retrieving raw queue inbound:', error);
    throw error;
  }
}

/**
 * Retrieve raw queue outbound data from database
 * @param {Object} filters - Filter criteria
 * @returns {Promise<Array>} - Array of raw queue outbound records
 */
export async function getRawQueueOutbound(filters = {}) {
  let sql = 'SELECT * FROM raw_queue_outbound WHERE 1=1';
  const params = [];
  
  // Normalize date parameters to support both naming conventions
  const { startDate, endDate } = normalizeDateParams(filters);
  
  if (startDate) {
    const startMs = convertTimestamp(startDate);
    sql += ' AND called_time >= ?';
    params.push(startMs);
  }
  
  if (endDate) {
    const endMs = convertTimestamp(endDate);
    sql += ' AND called_time <= ?';
    params.push(endMs);
  }
  
  if (filters.queueName) {
    sql += ' AND queue_name = ?';
    params.push(filters.queueName);
  }
  
  sql += ' ORDER BY called_time ASC';
  
  try {
    const rows = await query(sql, params);
    return rows.map(row => ({
      ...row,
      raw_data: typeof row.raw_data === 'string' ? JSON.parse(row.raw_data) : row.raw_data
    }));
  } catch (error) {
    console.error('Error retrieving raw queue outbound:', error);
    throw error;
  }
}


// Check if data exists in database for given date range
export async function checkDataExists(startDate, endDate) {
  console.log(`🔍 checkDataExists called with: startDate=${startDate}, endDate=${endDate}`);
  const startTs = convertTimestamp(startDate);
  const endTs = convertTimestamp(endDate);
  console.log(`🔍 Converted timestamps: startTs=${startTs}, endTs=${endTs}`);
  
  const tables = {
    'raw_campaigns': 'timestamp',
    'raw_queue_inbound': 'called_time', 
    'raw_queue_outbound': 'called_time',
    // 'raw_cdrs': 'timestamp',
  };
  
  const results = {};
  let totalRecords = 0;
  
  for (const [table, timestampCol] of Object.entries(tables)) {
    try {
      const sql = `SELECT COUNT(*) as count FROM ${table} WHERE ${timestampCol} >= ? AND ${timestampCol} <= ?`;
      console.log(`🔍 Checking ${table} with query: ${sql} [${startTs}, ${endTs}]`);
      const rows = await query(sql, [startTs, endTs]);
      const count = rows[0].count;
      console.log(`🔍 ${table}: found ${count} records`);
      results[table] = count;
      totalRecords += count;
    } catch (error) {
      console.error(`❌ Error checking ${table}:`, error);
      results[table] = 0;
    }
  }
  
  console.log(`🔍 checkDataExists result: hasData=${totalRecords > 0}, totalRecords=${totalRecords}`, results);
  return {
    hasData: totalRecords > 0,
    totalRecords,
    breakdown: results
  };
}

/**
 * Clear all cached data from database tables
 * @param {Object} options - Options for clearing cache
 * @returns {Promise<Object>} - Result object with cleared counts
 */
export async function clearCache(options = {}) {
  const tables = ['raw_campaigns', 'raw_queue_inbound', 'raw_queue_outbound'];
  const results = {};
  let totalCleared = 0;
  
  console.log('🗑️  Clearing cache memory...');
  
  for (const table of tables) {
    try {
      // Get count before clearing
      const countResult = await query(`SELECT COUNT(*) as count FROM ${table}`, []);
      const beforeCount = countResult[0].count;
      
      // Clear the table
      if (options.specificTable && options.specificTable !== table) {
        results[table] = { before: beforeCount, cleared: 0 };
        continue;
      }
      
      const clearResult = await query(`DELETE FROM ${table}`, []);
      const clearedCount = clearResult.affectedRows || beforeCount;
      
      results[table] = { before: beforeCount, cleared: clearedCount };
      totalCleared += clearedCount;
      
      console.log(`   ✅ ${table}: ${clearedCount} records cleared`);
    } catch (error) {
      console.error(`   ❌ Error clearing ${table}:`, error);
      results[table] = { error: error.message };
    }
  }
  
  console.log(`🧹 Cache cleared: ${totalCleared} total records removed`);
  
  return {
    success: true,
    totalCleared,
    breakdown: results
  };
}

/**
 * Clear specific table cache
 * @param {string} tableName - Name of table to clear
 * @returns {Promise<Object>} - Result object
 */
export async function clearTableCache(tableName) {
  const validTables = ['raw_campaigns', 'raw_queue_inbound', 'raw_queue_outbound'];
  
  if (!validTables.includes(tableName)) {
    throw new Error(`Invalid table name. Valid tables: ${validTables.join(', ')}`);
  }
  
  return await clearCache({ specificTable: tableName });
}

// /**
//  * Filter records by contact number directly in the database
//  * @param {Object} params - Filter parameters including contactNumber and date range
//  * @returns {Promise<Object>} - Object containing filtered records from all tables
//  */
async function getRecordsByContactNumber(params = {}) {
  const { contactNumber, startDate, endDate } = params;
  
  if (!contactNumber || typeof contactNumber !== 'string') {
    throw new Error('Contact number is required for filtering');
  }
  
  // Normalize the contact number for consistent matching
  const normalizedPhone = normalizePhoneNumber(contactNumber);
  if (!normalizedPhone) {
    throw new Error('Invalid contact number format');
  }
  
  console.log(`🔍 Filtering records by contact number: ${contactNumber} (normalized: ${normalizedPhone})`);
  
  // Convert date parameters to timestamps
  const startTs = startDate ? convertTimestamp(startDate) : null;
  const endTs = endDate ? convertTimestamp(endDate) : null;
  
  // Create SQL conditions for date filtering
  const dateConditions = {};
  if (startTs) {
    dateConditions.campaigns = 'AND c.timestamp >= ?';
    dateConditions.inbound = 'AND qi.called_time >= ?';
    dateConditions.outbound = 'AND qo.called_time >= ?';
    // dateConditions.cdrs = 'AND cdr.timestamp >= ?';
    // dateConditions.cdrsAll = 'AND cdrall.timestamp >= ?';
  } else {
    dateConditions.campaigns = '';
    dateConditions.inbound = '';
    dateConditions.outbound = '';
    // dateConditions.cdrs = '';
    // dateConditions.cdrsAll = '';
  }
  
  if (endTs) {
    dateConditions.campaigns += ' AND c.timestamp <= ?';
    dateConditions.inbound += ' AND qi.called_time <= ?';
    dateConditions.outbound += ' AND qo.called_time <= ?';
    // dateConditions.cdrs += ' AND cdr.timestamp <= ?';
    // dateConditions.cdrsAll += ' AND cdrall.timestamp <= ?';
  }
  
  // Create SQL queries for each table with JSON path expressions to search for the phone number
  // These queries use MySQL's JSON functions to extract and search within the raw_data JSON
  
  // Campaigns query - search in raw_data for contact number fields
  const campaignsQuery = `
    SELECT c.* FROM raw_campaigns c
    WHERE (
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(c.raw_data, '$.caller_id_number')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(c.raw_data, '$.to')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(c.raw_data, '$.from')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(c.raw_data, '$.destination_number')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(c.raw_data, '$.lead_number')), '') LIKE ?
    ) ${dateConditions.campaigns}
    ORDER BY c.timestamp DESC
  `;
  
  // Inbound queue query
  const inboundQuery = `
    SELECT qi.* FROM raw_queue_inbound qi
    WHERE (
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(qi.raw_data, '$.caller_id_number')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(qi.raw_data, '$.to')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(qi.raw_data, '$.from')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(qi.raw_data, '$.destination_number')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(qi.raw_data, '$.lead_number')), '') LIKE ?
    ) ${dateConditions.inbound}
    ORDER BY qi.called_time DESC
  `;
  
  // Outbound queue query
  const outboundQuery = `
    SELECT qo.* FROM raw_queue_outbound qo
    WHERE (
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(qo.raw_data, '$.caller_id_number')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(qo.raw_data, '$.to')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(qo.raw_data, '$.from')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(qo.raw_data, '$.destination_number')), '') LIKE ? OR
      IFNULL(JSON_UNQUOTE(JSON_EXTRACT(qo.raw_data, '$.lead_number')), '') LIKE ?
    ) ${dateConditions.outbound}
    ORDER BY qo.called_time DESC
  `;
  
  // // CDRs query
  // const cdrsQuery = `
  //   SELECT cdr.* FROM raw_cdrs cdr
  //   WHERE (
  //     IFNULL(JSON_UNQUOTE(JSON_EXTRACT(cdr.raw_data, '$.caller_id_number')), '') LIKE ? OR
  //     IFNULL(JSON_UNQUOTE(JSON_EXTRACT(cdr.raw_data, '$.to')), '') LIKE ? OR
  //     IFNULL(JSON_UNQUOTE(JSON_EXTRACT(cdr.raw_data, '$.from')), '') LIKE ? OR
  //     IFNULL(JSON_UNQUOTE(JSON_EXTRACT(cdr.raw_data, '$.destination_number')), '') LIKE ? OR
  //     IFNULL(JSON_UNQUOTE(JSON_EXTRACT(cdr.raw_data, '$.lead_number')), '') LIKE ?
  //   ) ${dateConditions.cdrs}
  //   ORDER BY cdr.timestamp DESC
  // `;
  
  // Create pattern for phone number matching using LIKE instead of REGEXP
  // This pattern will match the normalized phone number anywhere in the string
  const phonePattern = `%${normalizedPhone}%`;
  
  // Prepare parameters for each query
  // Each query needs the phone pattern 5 times (for each field)
  const campaignsParams = [phonePattern, phonePattern, phonePattern, phonePattern, phonePattern];
  const inboundParams = [phonePattern, phonePattern, phonePattern, phonePattern, phonePattern];
  const outboundParams = [phonePattern, phonePattern, phonePattern, phonePattern, phonePattern];
  // const cdrsParams = [phonePattern, phonePattern, phonePattern, phonePattern, phonePattern];

  // Add date parameters if provided
  if (startTs) {
    campaignsParams.push(startTs);
    inboundParams.push(startTs);
    outboundParams.push(startTs);
    // cdrsParams.push(startTs);
  }
  
  if (endTs) {
    campaignsParams.push(endTs);
    inboundParams.push(endTs);
    outboundParams.push(endTs);
    // cdrsParams.push(endTs);
  }
  
  // Execute all queries in parallel for better performance
  try {
    console.log('🔍 Executing database queries for contact number filtering...');
    
    const [campaignRecords, inboundRecords, outboundRecords] = await Promise.all([
      query(campaignsQuery, campaignsParams),
      query(inboundQuery, inboundParams),
      query(outboundQuery, outboundParams),
      // query(cdrsQuery, cdrsParams),
    ]);
    
    // Parse JSON data in results
    const parseJsonData = (records) => records.map(record => ({
      ...record,
      raw_data: typeof record.raw_data === 'string' ? JSON.parse(record.raw_data) : record.raw_data
    }));
    
    const parsedCampaignRecords = parseJsonData(campaignRecords);
    const parsedInboundRecords = parseJsonData(inboundRecords);
    const parsedOutboundRecords = parseJsonData(outboundRecords);
    // const parsedCdrRecords = parseJsonData(cdrRecords);;
    
    console.log(`✅ Database filtering complete. Found records:`);
    console.log(`   - Campaigns: ${parsedCampaignRecords.length}`);
    console.log(`   - Inbound: ${parsedInboundRecords.length}`);
    console.log(`   - Outbound: ${parsedOutboundRecords.length}`);
    // console.log(`   - CDRs: ${parsedCdrRecords.length}`);
    
    return {
      campaignRecords: parsedCampaignRecords,
      inboundRecords: parsedInboundRecords,
      outboundRecords: parsedOutboundRecords,
      // cdrRecords: parsedCdrRecords,
      totalRecords: parsedCampaignRecords.length + parsedInboundRecords.length + 
                   parsedOutboundRecords.length
    };
  } catch (error) {
    console.error('❌ Error filtering records by contact number:', error);
    throw error;
  }
}

/**
 * Close all connections in the pool
 * @returns {Promise<void>} - Promise that resolves when all connections are closed
 */
async function end() {
  try {
    console.log('Closing database connection pool...');
    await pool.end();
    console.log('Database connection pool closed successfully');
  } catch (error) {
    console.error('Error closing database connection pool:', error);
    throw error;
  }
}

// Create a default export object with all functions
const dbService = {
  query,
  batchInsert,
  beginTransaction,
  commitTransaction,
  rollbackTransaction,
  insertRawCampaigns,
  batchInsertRawCampaigns,
  insertRawQueueInbound,
  batchInsertRawQueueInbound,
  insertRawQueueOutbound,
  batchInsertRawQueueOutbound,
  // batchInsertRawCdrsAll,
  // getRawCdrsAll,
  // processRecordingsFromCdrsAll,
  // batchInsertRecordings,
  // getRecordings,
  getRecordsByContactNumber,
  getRawCampaigns,
  getRawQueueInbound,
  getRawQueueOutbound,
  checkDataExists,
  clearCache,
  clearTableCache,
  end
};

export default dbService;
