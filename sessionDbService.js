// sessionDbService.js
// Session-based database service for user isolation

import dbService from './dbService.js';

class SessionDbService {
  constructor() {
    this.sessionTables = new Map(); // sessionId -> table mappings
    this.sessionTimeout = 30 * 60 * 1000; // 30 minutes
  }

  // Create session-specific table names
  getSessionTableName(baseTableName, sessionId) {
    return `${baseTableName}_session_${sessionId}`;
  }

  // Initialize session tables for a user
  async initializeSessionTables(sessionId) {
    const baseTables = [
      // 'raw_cdrs',
      // 'raw_cdrs_all', 
      'raw_queue_inbound',
      'raw_queue_outbound',
      'raw_campaigns'
    ];

    const sessionTables = {};
    
    for (const baseTable of baseTables) {
      const sessionTable = this.getSessionTableName(baseTable, sessionId);
      sessionTables[baseTable] = sessionTable;
      
      // Create session table with same structure as base table
      await this.createSessionTable(baseTable, sessionTable);
    }

    this.sessionTables.set(sessionId, {
      tables: sessionTables,
      createdAt: Date.now(),
      lastAccessed: Date.now()
    });

    console.log(`📋 Session tables initialized for session: ${sessionId}`);
    return sessionTables;
  }

  // Create session table with same structure as base table
  async createSessionTable(baseTableName, sessionTableName) {
    try {
      // Drop table if exists
      await dbService.query(`DROP TABLE IF EXISTS ${sessionTableName}`);
      
      // Create table with same structure as base table
      await dbService.query(`CREATE TABLE ${sessionTableName} LIKE ${baseTableName}`);
      
      console.log(`✅ Created session table: ${sessionTableName}`);
    } catch (error) {
      console.error(`❌ Error creating session table ${sessionTableName}:`, error);
      throw error;
    }
  }

  // Get session table mappings
  getSessionTables(sessionId) {
    const sessionData = this.sessionTables.get(sessionId);
    if (!sessionData) {
      // If session not found in memory, try to reconstruct from database
      console.log(`⚠️ Session ${sessionId} not found in memory, reconstructing...`);
      return this.reconstructSessionTables(sessionId);
    }

    // Update last accessed time
    sessionData.lastAccessed = Date.now();
    return sessionData.tables;
  }

  // Reconstruct session tables from database (for worker threads)
  reconstructSessionTables(sessionId) {
    const baseTables = ['raw_queue_inbound', 'raw_queue_outbound', 'raw_campaigns'];
    const sessionTables = {};
    
    for (const baseTable of baseTables) {
      sessionTables[baseTable] = this.getSessionTableName(baseTable, sessionId);
    }
    
    // Store in memory for future use
    this.sessionTables.set(sessionId, {
      tables: sessionTables,
      createdAt: Date.now(),
      lastAccessed: Date.now()
    });
    
    return sessionTables;
  }

  // Session-aware batch insert functions

  async batchInsertRawQueueInbound(records, sessionId) {
    const sessionTables = this.getSessionTables(sessionId);
    const sessionTable = sessionTables.raw_queue_inbound;
    return await this.batchInsertToSessionTable(records, sessionTable, 'queue_inbound');
  }

  async batchInsertRawQueueOutbound(records, sessionId) {
    const sessionTables = this.getSessionTables(sessionId);
    const sessionTable = sessionTables.raw_queue_outbound;
    return await this.batchInsertToSessionTable(records, sessionTable, 'queue_outbound');
  }

  async batchInsertRawCampaigns(records, sessionId) {
    const sessionTables = this.getSessionTables(sessionId);
    const sessionTable = sessionTables.raw_campaigns;
    return await this.batchInsertToSessionTable(records, sessionTable, 'campaign');
  }

  // Generic batch insert to session table
  async batchInsertToSessionTable(records, sessionTable, recordType) {
    if (!records || records.length === 0) {
      return { inserted: 0, errors: 0 };
    }

    try {
      // Use the appropriate field mapping based on record type
      let query, values;

      switch (recordType) {
        case 'cdr':
        case 'cdr_all':
          query = `INSERT INTO ${sessionTable} (
            call_id, timestamp, queue_name, agent_name, agent_ext, caller_id_number,
            answered_time, hangup_time, wait_duration, agent_disposition, sub_disp_1,
            sub_disp_2, agent_history, queue_history, follow_up_notes, hold_duration, raw_data
          ) VALUES ?`;
          values = records.map(record => [
            record.call_id || record.callid || null,
            record.timestamp || null,
            record.queue_name || null,
            record.agent_name || null,
            record.agent_ext || null,
            record.caller_id_number || null,
            record.answered_time || null,
            record.hangup_time || null,
            record.wait_duration || null,
            record.agent_disposition || null,
            record.sub_disp_1 || null,
            record.sub_disp_2 || null,
            record.agent_history || null,
            record.queue_history || null,
            record.follow_up_notes || null,
            record.hold_duration || null,
            JSON.stringify(record)
          ]);
          break;

        case 'queue_inbound':
          query = `INSERT INTO ${sessionTable} (
            call_id, timestamp, queue_name, agent_name, agent_ext, caller_id_number,
            answered_time, hangup_time, wait_duration, agent_disposition, sub_disp_1,
            sub_disp_2, agent_history, queue_history, raw_data
          ) VALUES ?`;
          values = records.map(record => [
            record.call_id || record.callid || null,
            record.timestamp || null,
            record.queue_name || null,
            record.agent_name || null,
            record.agent_ext || null,
            record.caller_id_number || null,
            record.answered_time || null,
            record.hangup_time || null,
            record.wait_duration || null,
            record.agent_disposition || null,
            record.sub_disp_1 || null,
            record.sub_disp_2 || null,
            record.agent_history || null,
            record.queue_history || null,
            JSON.stringify(record)
          ]);
          break;

        case 'queue_outbound':
          query = `INSERT INTO ${sessionTable} (
            call_id, timestamp, queue_name, agent_name, agent_ext, caller_id_number,
            called_time, answered_time, hangup_time, wait_duration, agent_disposition,
            sub_disp_1, sub_disp_2, agent_history, queue_history, raw_data
          ) VALUES ?`;
          values = records.map(record => [
            record.call_id || record.callid || null,
            record.timestamp || null,
            record.queue_name || null,
            record.agent_name || null,
            record.agent_ext || null,
            record.caller_id_number || null,
            record.called_time || null,
            record.answered_time || null,
            record.hangup_time || null,
            record.wait_duration || null,
            record.agent_disposition || null,
            record.sub_disp_1 || null,
            record.sub_disp_2 || null,
            record.agent_history || null,
            record.queue_history || null,
            JSON.stringify(record)
          ]);
          break;

        case 'campaign':
          query = `INSERT INTO ${sessionTable} (
            call_id, timestamp, campaign_name, agent_name, agent_ext, caller_id_number,
            called_time, answered_time, hangup_time, wait_duration, agent_disposition,
            sub_disp_1, sub_disp_2, raw_data
          ) VALUES ?`;
          values = records.map(record => [
            record.call_id || record.callid || null,
            record.timestamp || null,
            record.campaign_name || null,
            record.agent_name || null,
            record.agent_ext || null,
            record.caller_id_number || null,
            record.called_time || null,
            record.answered_time || null,
            record.hangup_time || null,
            record.wait_duration || null,
            record.agent_disposition || null,
            record.sub_disp_1 || null,
            record.sub_disp_2 || null,
            JSON.stringify(record)
          ]);
          break;

        default:
          throw new Error(`Unknown record type: ${recordType}`);
      }

      const [result] = await dbService.query(query, [values]);
      return { inserted: result.affectedRows, errors: 0 };

    } catch (error) {
      console.error(`❌ Batch insert error for ${sessionTable}:`, error);
      return { inserted: 0, errors: records.length };
    }
  }

  // Get records from session tables
  async getSessionRecords(sessionId, tableType, limit = null) {
    const sessionTables = this.getSessionTables(sessionId);
    const sessionTable = sessionTables[tableType];
    
    if (!sessionTable) {
      throw new Error(`Unknown table type: ${tableType}`);
    }

    // Map table types to their correct timestamp column names
    const timestampColumns = {
      'raw_campaigns': 'timestamp',
      'raw_queue_inbound': 'called_time',
      'raw_queue_outbound': 'called_time'
    };

    const timestampColumn = timestampColumns[tableType] || 'id';
    const limitClause = limit ? `LIMIT ${limit}` : '';
    const query = `SELECT * FROM ${sessionTable} ORDER BY ${timestampColumn} DESC ${limitClause}`;
    
    const [rows] = await dbService.query(query);
    return rows;
  }

  // Get session statistics
  async getSessionStats(sessionId) {
    const sessionTables = this.getSessionTables(sessionId);
    const stats = {};

    // Map table types to their correct timestamp column names
    const timestampColumns = {
      'raw_campaigns': 'timestamp',
      'raw_queue_inbound': 'called_time',
      'raw_queue_outbound': 'called_time'
    };

    for (const [baseTable, sessionTable] of Object.entries(sessionTables)) {
      try {
        const countResult = await dbService.query(`SELECT COUNT(*) as count FROM ${sessionTable}`);
        
        const timestampColumn = timestampColumns[baseTable] || 'id';
        const timeResult = await dbService.query(`
          SELECT MIN(${timestampColumn}) as earliest, MAX(${timestampColumn}) as latest 
          FROM ${sessionTable} 
          WHERE ${timestampColumn} IS NOT NULL
        `);
        
        stats[baseTable] = {
          count: countResult[0]?.count || 0,
          earliest: timeResult[0]?.earliest || null,
          latest: timeResult[0]?.latest || null
        };
      } catch (error) {
        console.error(`Error getting stats for ${sessionTable}:`, error);
        stats[baseTable] = { count: 0, earliest: null, latest: null };
      }
    }

    return stats;
  }

  // Clean up expired sessions
  async cleanupExpiredSessions() {
    const now = Date.now();
    const expiredSessions = [];

    for (const [sessionId, sessionData] of this.sessionTables) {
      const age = now - sessionData.lastAccessed;
      if (age > this.sessionTimeout) {
        expiredSessions.push(sessionId);
      }
    }

    for (const sessionId of expiredSessions) {
      await this.dropSessionTables(sessionId);
    }

    if (expiredSessions.length > 0) {
      console.log(`🧹 Cleaned up ${expiredSessions.length} expired sessions`);
    }
  }

  // Get all active sessions
  getActiveSessions() {
    return Array.from(this.sessionTables.entries()).map(([sessionId, data]) => ({
      sessionId,
      createdAt: data.createdAt,
      lastAccessed: data.lastAccessed,
      age: Date.now() - data.createdAt,
      tables: Object.keys(data.tables)
    }));
  }

  // Check if global cache has data for date range
  async checkGlobalCacheForDateRange(startDate, endDate) {
    try {
      console.log(`🔍 Checking global cache for: ${startDate} to ${endDate}`);
      const { checkDataExists } = await import('./dbService.js');
      const result = await checkDataExists(startDate, endDate);
      console.log(`🔍 Global cache result:`, result);
      return result;
    } catch (error) {
      console.error('❌ Error checking global cache:', error);
      return { hasData: false, totalRecords: 0 };
    }
  }

  // Copy data from global tables to session tables for date range
  async copyGlobalDataToSession(sessionId, startDate, endDate) {
    const sessionTables = this.getSessionTables(sessionId);
    const copyResults = {};

    try {
      // Convert dates to timestamps for filtering
      const startTs = Math.floor(Date.parse(startDate) / 1000);
      const endTs = Math.floor(Date.parse(endDate) / 1000);

      console.log(`📋 Copying global data to session ${sessionId} for range: ${startDate} to ${endDate}`);

      // Copy data from each global table to corresponding session table
      const copyOperations = [
        { global: 'raw_queue_inbound', session: sessionTables['raw_queue_inbound'], timeCol: 'called_time' },
        { global: 'raw_queue_outbound', session: sessionTables['raw_queue_outbound'], timeCol: 'called_time' },
        { global: 'raw_campaigns', session: sessionTables['raw_campaigns'], timeCol: 'timestamp' }
      ];

      for (const op of copyOperations) {
        try {
          const copyQuery = `
            INSERT IGNORE INTO ${op.session} 
            SELECT * FROM ${op.global} 
            WHERE ${op.timeCol} >= ? AND ${op.timeCol} <= ?
          `;
          
          const result = await dbService.query(copyQuery, [startTs, endTs]);
          copyResults[op.global] = result.affectedRows || 0;
          
          console.log(`✅ Copied ${result.affectedRows} records from ${op.global} to ${op.session}`);
        } catch (error) {
          console.error(`❌ Error copying ${op.global}:`, error);
          copyResults[op.global] = 0;
        }
      }

      const totalCopied = Object.values(copyResults).reduce((sum, count) => sum + count, 0);
      console.log(`📊 Total records copied to session ${sessionId}: ${totalCopied}`);

      return copyResults;

    } catch (error) {
      console.error(`❌ Error copying global data to session ${sessionId}:`, error);
      throw error;
    }
  }

  // Create unified report from session-specific tables
  async createUnifiedReportFromSession(sessionId, account, params) {
    try {
      const sessionTables = this.getSessionTables(sessionId);
      
      // Get all records from session tables
      const [queueInboundRecords, queueOutboundRecords, campaignRecords] = await Promise.all([
        this.getSessionRecords(sessionId, 'raw_queue_inbound'),
        this.getSessionRecords(sessionId, 'raw_queue_outbound'),
        this.getSessionRecords(sessionId, 'raw_campaigns')
      ]);

      console.log(`📊 Session ${sessionId} records:`, {
        queue_inbound: queueInboundRecords?.length || 0,
        queue_outbound: queueOutboundRecords?.length || 0,
        campaigns: campaignRecords?.length || 0
      });

      // Import the report processing logic from reportFetcher
      const { createUnifiedReportFromDB } = await import('./reportFetcher.js');
      
      // Use the existing unified report logic but with session data
      const unifiedReport = await createUnifiedReportFromDB(account, params, {
        sessionId,
        sessionTables,
        sessionRecords: {
          queue_inbound: queueInboundRecords,
          queue_outbound: queueOutboundRecords,
          campaigns: campaignRecords
        }
      });

      return unifiedReport;

    } catch (error) {
      console.error(`❌ Error creating unified report for session ${sessionId}:`, error);
      throw error;
    }
  }
}

// Singleton instance
const sessionDbService = new SessionDbService();

// Cleanup expired sessions every 120 minutes
setInterval(() => {
  sessionDbService.cleanupExpiredSessions();
}, 120 * 60 * 1000);

export default sessionDbService;
