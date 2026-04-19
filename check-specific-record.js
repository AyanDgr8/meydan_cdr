import dbService from './dbService.js';

async function checkSpecificRecord() {
  try {
    const callId = '70edc468-acfe-123f-12a0-bc2411821d94';
    
    console.log(`\n🔍 Checking record: ${callId}\n`);
    
    // Check final_report
    const finalReportQuery = `
      SELECT call_id, agent_disposition, sub_disp_1, sub_disp_2, sub_disp_3, follow_up_notes, created_at, updated_at
      FROM final_report
      WHERE call_id = ?
    `;
    const finalReport = await dbService.query(finalReportQuery, [callId]);
    
    console.log('📊 FINAL_REPORT:');
    if (finalReport.length > 0) {
      console.log(JSON.stringify(finalReport[0], null, 2));
    } else {
      console.log('❌ Record not found in final_report');
    }
    
    // Check raw tables
    console.log('\n📊 Checking RAW TABLES:');
    
    const tables = [
      { name: 'raw_queue_inbound', idField: 'callid' },
      { name: 'raw_queue_outbound', idField: 'callid' },
      { name: 'raw_campaigns', idField: 'call_id' }
    ];
    
    for (const table of tables) {
      const rawQuery = `
        SELECT ${table.idField} as callid, created_at, updated_at, disposition_retry_count
        FROM ${table.name}
        WHERE ${table.idField} = ?
      `;
      const rawRecords = await dbService.query(rawQuery, [callId]);
      
      if (rawRecords.length > 0) {
        console.log(`\n✅ Found in ${table.name}:`);
        console.log(`   Created: ${rawRecords[0].created_at}`);
        console.log(`   Updated: ${rawRecords[0].updated_at}`);
        console.log(`   Retry Count: ${rawRecords[0].disposition_retry_count}`);
        
        // Get the raw_data to check disposition
        const dataQuery = `
          SELECT raw_data
          FROM ${table.name}
          WHERE ${table.idField} = ?
        `;
        const dataRecords = await dbService.query(dataQuery, [callId]);
        
        if (dataRecords.length > 0) {
          let rawData;
          try {
            rawData = typeof dataRecords[0].raw_data === 'string' 
              ? JSON.parse(dataRecords[0].raw_data) 
              : dataRecords[0].raw_data;
            
            console.log(`   agent_disposition: "${rawData.agent_disposition || ''}"`);
            console.log(`   fonouc_disposition_render exists: ${!!rawData.fonouc_disposition_render}`);
            console.log(`   follow_up_notes: "${(rawData.follow_up_notes || '').substring(0, 50)}${rawData.follow_up_notes?.length > 50 ? '...' : ''}"`);
            
            if (rawData.agent_subdisposition) {
              console.log(`   agent_subdisposition:`, JSON.stringify(rawData.agent_subdisposition, null, 2));
            }
          } catch (e) {
            console.log(`   ⚠️ Could not parse raw_data: ${e.message}`);
          }
        }
      }
    }
    
    process.exit(0);
  } catch (error) {
    console.error('❌ Error:', error);
    process.exit(1);
  }
}

checkSpecificRecord();
