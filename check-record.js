import dbService from './dbService.js';

async function checkRecord() {
  try {
    const callId = '8d77734a-acfc-123f-12a0-bc2411821d94';
    
    // Check final_report
    const finalReportQuery = `
      SELECT call_id, agent_disposition, sub_disp_1, sub_disp_2, sub_disp_3, follow_up_notes
      FROM final_report
      WHERE call_id = ?
    `;
    const finalReport = await dbService.query(finalReportQuery, [callId]);
    
    console.log('\n📊 FINAL_REPORT:');
    console.log(JSON.stringify(finalReport, null, 2));
    
    // Check raw tables
    const rawInboundQuery = `
      SELECT callid, raw_data
      FROM raw_queue_inbound
      WHERE callid = ?
    `;
    const rawInbound = await dbService.query(rawInboundQuery, [callId]);
    
    if (rawInbound.length > 0) {
      console.log('\n📊 RAW_QUEUE_INBOUND:');
      const rawData = JSON.parse(rawInbound[0].raw_data);
      console.log('agent_disposition:', rawData.agent_disposition);
      console.log('agent_subdisposition:', JSON.stringify(rawData.agent_subdisposition, null, 2));
      console.log('follow_up_notes:', rawData.follow_up_notes);
      console.log('fonouc_disposition_render exists:', !!rawData.fonouc_disposition_render);
    }
    
    process.exit(0);
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

checkRecord();
