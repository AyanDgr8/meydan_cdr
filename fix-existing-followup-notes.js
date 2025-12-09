// fix-existing-followup-notes.js
// One-time script to update existing final_report records with follow-up notes from raw tables

import dbService from './dbService.js';
import dotenv from 'dotenv';

dotenv.config();

async function fixExistingFollowUpNotes() {
  console.log('🔧 Starting one-time fix for existing follow-up notes...');
  
  try {
    // Find final_report records that have NULL follow_up_notes but corresponding raw records have follow-up notes
    const query = `
      SELECT 
        fr.call_id,
        fr.record_type,
        JSON_EXTRACT(rqo.raw_data, '$.fonoUC.follow_up_notes') as outbound_notes,
        JSON_EXTRACT(rqi.raw_data, '$.fonoUC.follow_up_notes') as inbound_notes,
        JSON_EXTRACT(rc.raw_data, '$.fonoUC.follow_up_notes') as campaign_notes
      FROM final_report fr
      LEFT JOIN raw_queue_outbound rqo ON fr.call_id = rqo.callid AND fr.record_type = 'Outbound'
      LEFT JOIN raw_queue_inbound rqi ON fr.call_id = rqi.callid AND fr.record_type = 'Inbound'  
      LEFT JOIN raw_campaigns rc ON fr.call_id = rc.call_id AND fr.record_type = 'Campaign'
      WHERE 
        (fr.follow_up_notes IS NULL OR fr.follow_up_notes = '')
        AND (
          (fr.record_type = 'Outbound' AND JSON_EXTRACT(rqo.raw_data, '$.fonoUC.follow_up_notes') IS NOT NULL AND JSON_EXTRACT(rqo.raw_data, '$.fonoUC.follow_up_notes') != '')
          OR (fr.record_type = 'Inbound' AND JSON_EXTRACT(rqi.raw_data, '$.fonoUC.follow_up_notes') IS NOT NULL AND JSON_EXTRACT(rqi.raw_data, '$.fonoUC.follow_up_notes') != '')
          OR (fr.record_type = 'Campaign' AND JSON_EXTRACT(rc.raw_data, '$.fonoUC.follow_up_notes') IS NOT NULL AND JSON_EXTRACT(rc.raw_data, '$.fonoUC.follow_up_notes') != '')
        )
      LIMIT 1000
    `;
    
    console.log('📊 Finding final_report records missing follow-up notes...');
    const recordsToFix = await dbService.query(query);
    
    console.log(`📊 Found ${recordsToFix.length} records to fix`);
    
    if (recordsToFix.length === 0) {
      console.log('✅ No records need fixing');
      return;
    }
    
    let fixedCount = 0;
    
    for (const record of recordsToFix) {
      try {
        let followUpNotes = '';
        
        // Extract follow-up notes based on record type
        if (record.record_type === 'Outbound') {
          followUpNotes = record.outbound_notes || '';
        } else if (record.record_type === 'Inbound') {
          followUpNotes = record.inbound_notes || '';
        } else if (record.record_type === 'Campaign') {
          followUpNotes = record.campaign_notes || '';
        }
        
        // Clean up follow-up notes (remove quotes if present)
        followUpNotes = followUpNotes.replace(/^"|"$/g, '');
        
        if (followUpNotes && followUpNotes !== '') {
          // Update final_report record
          const updateQuery = `
            UPDATE final_report 
            SET 
              follow_up_notes = ?,
              updated_at = NOW()
            WHERE call_id = ?
          `;
          
          await dbService.query(updateQuery, [
            followUpNotes,
            record.call_id
          ]);
          
          fixedCount++;
          console.log(`  ✅ Fixed ${record.call_id} (${record.record_type}): "${followUpNotes.substring(0, 50)}${followUpNotes.length > 50 ? '...' : ''}"`);
        }
        
      } catch (error) {
        console.error(`  ❌ Failed to fix ${record.call_id}: ${error.message}`);
      }
    }
    
    console.log(`🎉 Successfully fixed ${fixedCount} records with follow-up notes in final_report table`);
    
  } catch (error) {
    console.error('❌ Error in follow-up notes fix script:', error);
  }
  
  process.exit(0);
}

fixExistingFollowUpNotes();
