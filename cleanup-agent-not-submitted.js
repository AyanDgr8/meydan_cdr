// cleanup-agent-not-submitted.js
// Script to clean up existing "AGENT NOT SUBMITTED" records with follow-up notes

import dbService from './dbService.js';

async function cleanupAgentNotSubmittedRecords() {
  console.log('🧹 Starting cleanup of AGENT NOT SUBMITTED records...');
  
  try {
    // Find all records with "AGENT NOT SUBMITTED" that have follow-up notes or sub-dispositions
    const findQuery = `
      SELECT call_id, agent_disposition, sub_disp_1, sub_disp_2, sub_disp_3, follow_up_notes
      FROM final_report
      WHERE agent_disposition = 'AGENT NOT SUBMITTED'
      AND (
        follow_up_notes IS NOT NULL 
        OR follow_up_notes != ''
        OR sub_disp_1 IS NOT NULL
        OR sub_disp_1 != ''
        OR sub_disp_2 IS NOT NULL
        OR sub_disp_2 != ''
        OR sub_disp_3 IS NOT NULL
        OR sub_disp_3 != ''
      )
    `;
    
    const recordsToClean = await dbService.query(findQuery);
    console.log(`📊 Found ${recordsToClean.length} records with "AGENT NOT SUBMITTED" that have follow-up notes or sub-dispositions`);
    
    if (recordsToClean.length === 0) {
      console.log('✅ No records need cleaning!');
      return;
    }
    
    // Show sample records
    console.log('\n📋 Sample records to be cleaned:');
    recordsToClean.slice(0, 5).forEach(record => {
      console.log(`  - ${record.call_id}: sub_disp_1="${record.sub_disp_1}", follow_up_notes="${record.follow_up_notes?.substring(0, 50)}..."`);
    });
    
    // Clean up these records
    const updateQuery = `
      UPDATE final_report
      SET sub_disp_1 = NULL,
          sub_disp_2 = NULL,
          sub_disp_3 = NULL,
          follow_up_notes = NULL,
          updated_at = NOW()
      WHERE agent_disposition = 'AGENT NOT SUBMITTED'
      AND (
        follow_up_notes IS NOT NULL 
        OR follow_up_notes != ''
        OR sub_disp_1 IS NOT NULL
        OR sub_disp_1 != ''
        OR sub_disp_2 IS NOT NULL
        OR sub_disp_2 != ''
        OR sub_disp_3 IS NOT NULL
        OR sub_disp_3 != ''
      )
    `;
    
    const result = await dbService.query(updateQuery);
    console.log(`\n✅ Successfully cleaned ${result.affectedRows} records`);
    console.log('   - Set sub_disp_1, sub_disp_2, sub_disp_3, follow_up_notes to NULL');
    
    // Verify cleanup
    const verifyQuery = `
      SELECT COUNT(*) as count
      FROM final_report
      WHERE agent_disposition = 'AGENT NOT SUBMITTED'
      AND (
        follow_up_notes IS NOT NULL 
        OR follow_up_notes != ''
        OR sub_disp_1 IS NOT NULL
        OR sub_disp_1 != ''
      )
    `;
    
    const verifyResult = await dbService.query(verifyQuery);
    const remainingCount = verifyResult[0].count;
    
    if (remainingCount === 0) {
      console.log('\n✅ Cleanup verified! All "AGENT NOT SUBMITTED" records now have NULL sub-dispositions and follow-up notes');
    } else {
      console.log(`\n⚠️ Warning: ${remainingCount} records still have issues. Manual review may be needed.`);
    }
    
  } catch (error) {
    console.error('❌ Error during cleanup:', error);
    throw error;
  }
}

// Run the cleanup
cleanupAgentNotSubmittedRecords()
  .then(() => {
    console.log('\n✅ Cleanup completed successfully!');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n❌ Cleanup failed:', error);
    process.exit(1);
  });
