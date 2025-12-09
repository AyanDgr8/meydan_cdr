// // recordingsFetcher.js
// // Specialized fetcher for processing raw CDRs All data into recordings format
// // Similar to reportFetcher.js but focused on recordings extraction and processing

// import dbService from './dbService.js';

// /**
//  * Process raw CDRs All data and extract recordings with proper formatting
//  * @param {Object} filters - Filter criteria (startDate, endDate, etc.)
//  * @returns {Promise<Array>} - Array of processed recording objects
//  */
// export async function fetchAndProcessRecordings(filters = {}) {
//   console.log('🎵 Starting recordings fetch and processing...');
  
//   try {
//     // Step 1: Get raw CDRs All data from database
//     console.log('📊 Step 1: Fetching raw CDRs All data...');
//     const rawCdrsAllData = await dbService.getRawCdrsAll(filters);
//     console.log(`📊 Found ${rawCdrsAllData.length} raw CDRs All records`);
    
//     if (rawCdrsAllData.length === 0) {
//       console.log('ℹ️ No raw CDRs All data found for the given filters');
//       return [];
//     }
    
//     // Step 2: Process and extract recordings
//     console.log('📊 Step 2: Processing recordings from raw data...');
//     const processedRecordings = [];
//     let totalRecordingsFound = 0;
//     let recordsWithRecordings = 0;
    
//     for (const record of rawCdrsAllData) {
//       const rawData = record.raw_data;
      
//       // Extract media recordings from custom_channel_vars
//       const mediaRecordings = rawData.custom_channel_vars?.media_recordings;
      
//       if (mediaRecordings) {
//         recordsWithRecordings++;
        
//         // Handle both array and string formats
//         let recordingIds = '';
        
//         if (Array.isArray(mediaRecordings)) {
//           recordingIds = mediaRecordings.filter(id => id && id.trim() !== '').join(',');
//         } else if (typeof mediaRecordings === 'string' && mediaRecordings.trim() !== '') {
//           recordingIds = mediaRecordings.trim();
//         }
        
//         // Process recording IDs (now as comma-separated string)
//         if (recordingIds) {
//           totalRecordingsFound++;
          
//           const processedRecording = {
//             // Core identification
//             call_id: rawData.call_id || '',
//             recording_id: recordingIds,
            
//             // Timestamp information
//             timestamp: convertToUnixTimestamp(rawData.timestamp),
//             called_time: convertToUnixTimestamp(rawData.timestamp),
//             called_time_formatted: formatTimestamp(rawData.timestamp),
            
//             // Caller information
//             caller_id_number: cleanPhoneNumber(rawData.caller_id_number) || '',
//             caller_id_name: cleanCallerName(rawData.caller_id_name) || '',
            
//             // Callee information
//             callee_id_number: cleanPhoneNumber(rawData.callee_id_number) || '',
//             callee_id_name: rawData.callee_id_name || '',
            
//             // Additional metadata for context
//             bridge_id: rawData.custom_channel_vars?.bridge_id || '',
            

//             // Raw data reference for debugging
//             raw_record_id: record.call_id
//           };
          
//           processedRecordings.push(processedRecording);
//         }
//       }
//     }
    
//     console.log(`📊 Processing complete:`);
//     console.log(`   - Records processed: ${rawCdrsAllData.length}`);
//     console.log(`   - Records with recordings: ${recordsWithRecordings}`);
//     console.log(`   - Total recordings extracted: ${totalRecordingsFound}`);
    
//     // Step 3: Sort recordings by timestamp (newest first)
//     processedRecordings.sort((a, b) => {
//       const timeA = a.timestamp || 0;
//       const timeB = b.timestamp || 0;
//       return timeB - timeA;
//     });
    
//     console.log('✅ Recordings processing completed successfully');
//     return processedRecordings;
    
//   } catch (error) {
//     console.error('❌ Error in fetchAndProcessRecordings:', error);
//     throw error;
//   }
// }

// /**
//  * Format timestamp using the same logic as reportFetcher.js
//  * @param {number|string} ts - Raw timestamp
//  * @returns {string} - Formatted timestamp string
//  */
// function formatTimestamp(ts) {
//   if (ts == null || ts === '' || ts === undefined) return '';
  
//   try {
//     // Handle different timestamp formats
//     let timestamp = ts;
//     if (typeof ts === 'string') {
//       timestamp = parseFloat(ts);
//       // Check if parsing resulted in NaN
//       if (isNaN(timestamp)) return '';
//     }
    
//     // Validate timestamp is a number
//     if (typeof timestamp !== 'number' || isNaN(timestamp)) return '';
    
//     // Handle Gregorian timestamps (convert to Unix timestamp)
//     if (timestamp > 60000000000) {
//       const unixSeconds = timestamp - 62167219200;
//       timestamp = unixSeconds * 1000; // Convert to milliseconds
//     } else if (timestamp < 10_000_000_000) {
//       // Convert Unix seconds to milliseconds
//       timestamp = timestamp * 1000;
//     }
    
//     // Validate timestamp range (reasonable date range)
//     if (timestamp < 0 || timestamp > 4102444800000) { // Year 2100
//       return '';
//     }
    
//     // Create date and format to local time
//     const date = new Date(timestamp);
//     if (isNaN(date.getTime())) return '';
    
//     return date.toLocaleString('en-GB', { 
//       timeZone: 'Asia/Dubai',
//       day: '2-digit',
//       month: '2-digit', 
//       year: 'numeric',
//       hour: '2-digit',
//       minute: '2-digit',
//       second: '2-digit',
//       hour12: false
//     });
//   } catch (error) {
//     console.warn('Error formatting timestamp:', ts, error);
//     return '';
//   }
// }

// /**
//  * Convert timestamp to Unix timestamp in seconds for database storage
//  * @param {number|string} ts - Raw timestamp
//  * @returns {number|null} - Unix timestamp in seconds
//  */
// function convertToUnixTimestamp(ts) {
//   if (ts == null || ts === '' || ts === undefined) return null;
  
//   try {
//     let timestamp = ts;
//     if (typeof ts === 'string') {
//       timestamp = parseFloat(ts);
//       if (isNaN(timestamp)) return null;
//     }
    
//     if (typeof timestamp !== 'number' || isNaN(timestamp)) return null;
    
//     // Handle Gregorian timestamps (convert to Unix timestamp)
//     if (timestamp > 60000000000) {
//       const unixSeconds = timestamp - 62167219200;
//       return unixSeconds; // Return seconds
//     } else if (timestamp < 10_000_000_000) {
//       // Already in Unix seconds
//       return timestamp;
//     } else {
//       // Unix milliseconds, convert to seconds
//       return Math.floor(timestamp / 1000);
//     }
//   } catch (error) {
//     console.warn('Error converting timestamp:', ts, error);
//     return null;
//   }
// }

// /**
//  * Clean and format phone numbers
//  * @param {string} phoneNumber - Raw phone number
//  * @returns {string} - Cleaned phone number
//  */
// function cleanPhoneNumber(phoneNumber) {
//   if (!phoneNumber || typeof phoneNumber !== 'string') {
//     return '';
//   }
  
//   // Remove quotes and extra characters
//   let cleaned = phoneNumber.replace(/['"]/g, '').trim();
  
//   // Handle special formats like "+97147777242" or "97147777242"
//   if (cleaned.startsWith('+')) {
//     return cleaned;
//   } else if (cleaned.match(/^\d+$/)) {
//     // If it's all digits and looks like an international number, add +
//     if (cleaned.length > 10 && !cleaned.startsWith('0')) {
//       return `+${cleaned}`;
//     }
//   }
  
//   return cleaned;
// }

// /**
//  * Clean and format caller names
//  * @param {string} callerName - Raw caller name
//  * @returns {string} - Cleaned caller name
//  */
// function cleanCallerName(callerName) {
//   if (!callerName || typeof callerName !== 'string') {
//     return '';
//   }
  
//   // Remove quotes and extra characters
//   let cleaned = callerName.replace(/['"\\]/g, '').trim();
  
//   // Remove common prefixes like "prv-dialer-"
//   cleaned = cleaned.replace(/^prv-dialer-/, '');
  
//   return cleaned;
// }

// /**
//  * Apply additional filters to processed recordings
//  * @param {Array} recordings - Array of processed recordings
//  * @param {Object} filters - Additional filters to apply
//  * @returns {Array} - Filtered recordings
//  */
// export function applyRecordingsFilters(recordings, filters = {}) {
//   let filteredRecordings = [...recordings];
  
//   // Filter by caller ID number
//   if (filters.callerIdNumber) {
//     const searchTerm = filters.callerIdNumber.toLowerCase();
//     filteredRecordings = filteredRecordings.filter(recording => 
//       recording.caller_id_number.toLowerCase().includes(searchTerm)
//     );
//   }
  
//   // Filter by callee ID number
//   if (filters.calleeIdNumber) {
//     const searchTerm = filters.calleeIdNumber.toLowerCase();
//     filteredRecordings = filteredRecordings.filter(recording => 
//       recording.callee_id_number.toLowerCase().includes(searchTerm)
//     );
//   }
  
//   // Filter by call ID
//   if (filters.callId) {
//     const searchTerm = filters.callId.toLowerCase();
//     filteredRecordings = filteredRecordings.filter(recording => 
//       recording.call_id.toLowerCase().includes(searchTerm)
//     );
//   }
  
//   // Filter by recording ID
//   if (filters.recordingId) {
//     const searchTerm = filters.recordingId.toLowerCase();
//     filteredRecordings = filteredRecordings.filter(recording => 
//       recording.recording_id.toLowerCase().includes(searchTerm)
//     );
//   }
  
//   return filteredRecordings;
// }

// /**
//  * Get recordings statistics
//  * @param {Array} recordings - Array of processed recordings
//  * @returns {Object} - Statistics object
//  */
// export function getRecordingsStatistics(recordings) {
//   const stats = {
//     totalRecordings: recordings.length,
//     uniqueCalls: new Set(recordings.map(r => r.call_id)).size,
//     callDirections: {},
//     dispositions: {},
//     dateRange: {
//       earliest: null,
//       latest: null
//     }
//   };
  
//   recordings.forEach(recording => { 

//     // Track date range
//     if (recording.timestamp) {
//       if (!stats.dateRange.earliest || recording.timestamp < stats.dateRange.earliest) {
//         stats.dateRange.earliest = recording.timestamp;
//       }
//       if (!stats.dateRange.latest || recording.timestamp > stats.dateRange.latest) {
//         stats.dateRange.latest = recording.timestamp;
//       }
//     }
//   });
  
//   return stats;
// }

// /**
//  * Export recordings to CSV format
//  * @param {Array} recordings - Array of processed recordings
//  * @returns {string} - CSV content
//  */
// export function exportRecordingsToCSV(recordings) {
//   const headers = [
//     'Called Time',
//     'Caller ID Number',
//     'Caller ID Name',
//     'Callee ID Number',
//     'Callee ID Name',
//     'Call ID',
//     'Recording ID'
//   ];
  
//   let csvContent = headers.join(',') + '\n';
  
//   recordings.forEach(recording => {
//     const row = [
//       `"${(recording.called_time_formatted || '').replace(/"/g, '""')}"`,
//       `"${(recording.caller_id_number || '').replace(/"/g, '""')}"`,
//       `"${(recording.caller_id_name || '').replace(/"/g, '""')}"`,
//       `"${(recording.callee_id_number || '').replace(/"/g, '""')}"`,
//       `"${(recording.callee_id_name || '').replace(/"/g, '""')}"`,
//       `"${(recording.call_id || '').replace(/"/g, '""')}"`,
//       `"${(recording.recording_id || '').replace(/"/g, '""')}"`,
//     ].join(',');
    
//     csvContent += row + '\n';
//   });
  
//   return csvContent;
// }

// export default {
//   fetchAndProcessRecordings,
//   applyRecordingsFilters,
//   getRecordingsStatistics,
//   exportRecordingsToCSV
// };
