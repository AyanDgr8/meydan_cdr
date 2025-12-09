// fix-existing-dispositions.js
// One-time script to update existing final_report records with disposition information from raw tables

import dbService from './dbService.js';
import dotenv from 'dotenv';

dotenv.config();

async function fixExistingDispositions() {
  console.log('🔧 Starting one-time fix for existing disposition fields...');
  
  try {
    // Find final_report records that have NULL disposition but corresponding raw records have disposition
    const query = `
      SELECT 
        fr.call_id,
        fr.record_type,
        JSON_EXTRACT(rqo.raw_data, '$.agent_disposition') as outbound_disposition,
        JSON_EXTRACT(rqo.raw_data, '$.agent_subdisposition.name') as outbound_subdisposition,
        JSON_EXTRACT(rqi.raw_data, '$.agent_disposition') as inbound_disposition,
        JSON_EXTRACT(rqi.raw_data, '$.agent_subdisposition.name') as inbound_subdisposition,
        JSON_EXTRACT(rc.raw_data, '$.agent_disposition') as campaign_disposition,
        JSON_EXTRACT(rc.raw_data, '$.agent_subdisposition.name') as campaign_subdisposition
      FROM final_report fr
      LEFT JOIN raw_queue_outbound rqo ON fr.call_id = rqo.callid AND fr.record_type = 'Outbound'
      LEFT JOIN raw_queue_inbound rqi ON fr.call_id = rqi.callid AND fr.record_type = 'Inbound'  
      LEFT JOIN raw_campaigns rc ON fr.call_id = rc.call_id AND fr.record_type = 'Campaign'
      WHERE 
        fr.agent_disposition IS NULL
        AND (
          (fr.record_type = 'Outbound' AND JSON_EXTRACT(rqo.raw_data, '$.agent_disposition') IS NOT NULL AND JSON_EXTRACT(rqo.raw_data, '$.agent_disposition') != '')
          OR (fr.record_type = 'Inbound' AND JSON_EXTRACT(rqi.raw_data, '$.agent_disposition') IS NOT NULL AND JSON_EXTRACT(rqi.raw_data, '$.agent_disposition') != '')
          OR (fr.record_type = 'Campaign' AND JSON_EXTRACT(rc.raw_data, '$.agent_disposition') IS NOT NULL AND JSON_EXTRACT(rc.raw_data, '$.agent_disposition') != '')
        )
      LIMIT 1000
    `;
    
    console.log('📊 Finding final_report records missing disposition information...');
    const recordsToFix = await dbService.query(query);
    
    console.log(`📊 Found ${recordsToFix.length} records to fix`);
    
    if (recordsToFix.length === 0) {
      console.log('✅ No records need fixing');
      return;
    }
    
    let fixedCount = 0;
    
    for (const record of recordsToFix) {
      try {
        let agentDisposition = '';
        let agentSubdisposition = '';
        
        // Extract disposition based on record type
        if (record.record_type === 'Outbound') {
          agentDisposition = record.outbound_disposition || '';
          agentSubdisposition = record.outbound_subdisposition || '';
        } else if (record.record_type === 'Inbound') {
          agentDisposition = record.inbound_disposition || '';
          agentSubdisposition = record.inbound_subdisposition || '';
        } else if (record.record_type === 'Campaign') {
          agentDisposition = record.campaign_disposition || '';
          agentSubdisposition = record.campaign_subdisposition || '';
        }
        
        // Clean up disposition values (remove quotes if present)
        agentDisposition = agentDisposition.replace(/^"|"$/g, '');
        agentSubdisposition = agentSubdisposition.replace(/^"|"$/g, '');
        
        if (agentDisposition && agentDisposition !== '') {
          // Update final_report record
          const updateQuery = `
            UPDATE final_report 
            SET 
              agent_disposition = ?,
              sub_disp_1 = ?,
              sub_disp_2 = ?,
              updated_at = NOW()
            WHERE call_id = ?
          `;
          
          await dbService.query(updateQuery, [
            agentDisposition,
            agentSubdisposition,
            agentSubdisposition,
            record.call_id
          ]);
          
          fixedCount++;
          console.log(`  ✅ Fixed ${record.call_id} (${record.record_type}): "${agentDisposition}" / "${agentSubdisposition}"`);
        }
        
      } catch (error) {
        console.error(`  ❌ Failed to fix ${record.call_id}: ${error.message}`);
      }
    }
    
    console.log(`🎉 Successfully fixed ${fixedCount} records in final_report table`);
    
  } catch (error) {
    console.error('❌ Error in fix script:', error);
  }
  
  process.exit(0);
}

fixExistingDispositions();
