// Check if the call_id exists anywhere and what its timestamp is
import dbService from './dbService.js';

async function findRecord() {
  try {
    const callId = '70edc468-acfe-123f-12a0-bc2411821d94';
    
    console.log(`\n🔍 Searching for record: ${callId}\n`);
    
    // Search in all possible tables
    const tables = [
      { name: 'raw_queue_inbound', idField: 'callid' },
      { name: 'raw_queue_outbound', idField: 'callid' },
      { name: 'raw_campaigns', idField: 'call_id' },
      { name: 'final_report', idField: 'call_id' }
    ];
    
    let found = false;
    
    for (const table of tables) {
      const query = `
        SELECT COUNT(*) as count
        FROM ${table.name}
        WHERE ${table.idField} = ?
      `;
      const result = await dbService.query(query, [callId]);
      
      if (result[0].count > 0) {
        console.log(`✅ Found in ${table.name}`);
        
        // Get details
        const detailQuery = `
          SELECT *
          FROM ${table.name}
          WHERE ${table.idField} = ?
          LIMIT 1
        `;
        const details = await dbService.query(detailQuery, [callId]);
        console.log('Details:', JSON.stringify(details[0], null, 2));
        found = true;
      } else {
        console.log(`❌ Not found in ${table.name}`);
      }
    }
    
    if (!found) {
      console.log('\n⚠️ Record not found in any table!');
      console.log('\nPossible reasons:');
      console.log('1. The record timestamp is outside the range you populated (1775505600 - 1775591940)');
      console.log('2. The record was never fetched from the API');
      console.log('3. The call_id might be incorrect');
      console.log('\nTo verify, please check:');
      console.log('- What timestamp range does this call_id belong to?');
      console.log('- Is this an Inbound, Outbound, or Campaign call?');
    }
    
    process.exit(0);
  } catch (error) {
    console.error('❌ Error:', error);
    process.exit(1);
  }
}

findRecord();
