import mysql from 'mysql2/promise';
import { exec } from 'child_process';
import { promisify } from 'util';
import path from 'path';
import fs from 'fs';
import dotenv from 'dotenv';

dotenv.config();

const execAsync = promisify(exec);
const SSH_CONTROL_PATH = path.join(process.cwd(), '.ssh-control-%r@%h:%p');

// Test with a specific phone number
const TEST_PHONE = '502092505';

async function establishSSHMaster() {
  const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
  
  console.log('Establishing SSH connection...');
  
  const sshMasterCmd = `ssh -i "${pemPath}" -o ControlMaster=yes -o ControlPath="${SSH_CONTROL_PATH}" -o ControlPersist=10m -p 11446 root@94.206.56.70 -N -f`;
  
  try {
    await execAsync(sshMasterCmd);
    console.log('✅ SSH master connection established');
    return true;
  } catch (error) {
    if (error.message.includes('Control socket connect')) {
      console.log('✅ SSH master connection already exists');
      return true;
    }
    console.error('❌ Failed to establish SSH connection:', error.message);
    return false;
  }
}

async function closeSSHMaster() {
  const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
  const closeCmd = `ssh -i "${pemPath}" -o ControlPath="${SSH_CONTROL_PATH}" -O exit -p 11446 root@94.206.56.70 2>/dev/null`;
  
  try {
    await execAsync(closeCmd);
    console.log('SSH master connection closed');
  } catch (error) {
    // Ignore errors when closing
  }
}

async function testLocalRecord() {
  console.log('\n=== STEP 1: Check Local Database ===');
  
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'Ayan@1012',
    database: 'meydan_main_cdr',
    port: 3306
  });
  
  const [rows] = await connection.execute(
    `SELECT id, call_id, record_type, caller_id_number, callee_id_number, agent_name, agent_disposition, hangup_time 
     FROM final_report 
     WHERE caller_id_number LIKE ? OR callee_id_number LIKE ? 
     ORDER BY id DESC LIMIT 3`,
    [`%${TEST_PHONE}%`, `%${TEST_PHONE}%`]
  );
  
  console.log(`Found ${rows.length} records for phone ${TEST_PHONE}:`);
  rows.forEach(row => {
    console.log(`\n  ID: ${row.id}`);
    console.log(`  Call ID: ${row.call_id}`);
    console.log(`  Record Type: ${row.record_type}`);
    console.log(`  Caller: ${row.caller_id_number}`);
    console.log(`  Callee: ${row.callee_id_number}`);
    console.log(`  Agent: ${row.agent_name}`);
    console.log(`  Disposition: ${row.agent_disposition || 'NULL'}`);
    console.log(`  Hangup Time: ${row.hangup_time}`);
  });
  
  await connection.end();
  return rows[0];
}

async function testFormsNew(phone) {
  console.log('\n=== STEP 2: Check forms_new Table ===');
  
  const normalizedPhone = phone.replace(/\D/g, '').replace(/^0+/, '');
  console.log(`Normalized phone: ${normalizedPhone}`);
  
  const sqlQuery = `SELECT contact_number, call_type, disposition_1, disposition_2, disposition_3, query, created_at FROM forms_new WHERE (REPLACE(REPLACE(REPLACE(contact_number,' ',''),'-',''),'+','') LIKE '%${normalizedPhone}' OR REPLACE(REPLACE(REPLACE(contact_number,' ',''),'-',''),'+','') LIKE '${normalizedPhone}%') ORDER BY created_at DESC LIMIT 3`;
  
  const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
  const escapedQuery = sqlQuery.replace(/'/g, "'\\''");
  const sshCommand = `ssh -i "${pemPath}" -o ControlPath="${SSH_CONTROL_PATH}" -p 11446 root@94.206.56.70 "ssh my02 \\"docker exec mysql mysql -umeydanuser -pAyan@1012 meydanform -e '${escapedQuery}' -s -N\\""`;
  
  console.log('\nExecuting forms_new query...');
  
  try {
    const { stdout, stderr } = await execAsync(sshCommand, {
      timeout: 10000,
      maxBuffer: 1024 * 1024
    });
    
    const output = stdout.trim();
    
    if (!output) {
      console.log('❌ No records found in forms_new');
      return null;
    }
    
    console.log('✅ Found records in forms_new:');
    const lines = output.split('\n');
    lines.forEach((line, idx) => {
      const values = line.split('\t');
      console.log(`\n  Record ${idx + 1}:`);
      console.log(`    Contact: ${values[0]}`);
      console.log(`    Call Type: ${values[1]}`);
      console.log(`    Disposition 1: ${values[2]}`);
      console.log(`    Disposition 2: ${values[3]}`);
      console.log(`    Created At: ${values[6]}`);
    });
    
    return output;
  } catch (error) {
    console.error('❌ Error querying forms_new:', error.message);
    return null;
  }
}

async function testWebhookLogs(callId) {
  console.log('\n=== STEP 3: Check webhook_logs Table ===');
  console.log(`Call ID: ${callId}`);
  
  if (!callId) {
    console.log('❌ No call_id provided');
    return null;
  }
  
  const sqlQuery = `SELECT id, call_id, payload, created_at FROM webhook_logs WHERE call_id = '${callId}' AND payload IS NOT NULL ORDER BY created_at DESC LIMIT 3`;
  
  const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
  const escapedQuery = sqlQuery.replace(/'/g, "'\\''");
  const sshCommand = `ssh -i "${pemPath}" -o ControlPath="${SSH_CONTROL_PATH}" -p 11446 root@94.206.56.70 "ssh my02 \\"docker exec mysql mysql -umeydanuser -pAyan@1012 meydanform -e '${escapedQuery}' -s\\""`;
  
  console.log('\nExecuting webhook_logs query...');
  
  try {
    const { stdout, stderr } = await execAsync(sshCommand, {
      timeout: 10000,
      maxBuffer: 1024 * 1024
    });
    
    const output = stdout.trim();
    
    if (!output) {
      console.log('❌ No records found in webhook_logs');
      return null;
    }
    
    console.log('✅ Found records in webhook_logs:');
    const lines = output.split('\n');
    lines.forEach((line, idx) => {
      const values = line.split('\t');
      console.log(`\n  Record ${idx + 1}:`);
      console.log(`    ID: ${values[0]}`);
      console.log(`    Call ID: ${values[1]}`);
      console.log(`    Created At: ${values[3]}`);
      
      try {
        const payload = JSON.parse(values[2]);
        console.log(`    Payload:`);
        console.log(`      - disposition: ${payload.disposition}`);
        console.log(`      - subdisposition: ${payload.subdisposition?.name}`);
        console.log(`      - sub-subdisposition: ${payload.subdisposition?.subdisposition?.name}`);
        console.log(`      - follow_up_notes: ${payload.follow_up_notes}`);
      } catch (e) {
        console.log(`    Payload (raw): ${values[2].substring(0, 100)}...`);
      }
    });
    
    return output;
  } catch (error) {
    console.error('❌ Error querying webhook_logs:', error.message);
    return null;
  }
}

async function main() {
  let sshEstablished = false;
  
  try {
    const sshSuccess = await establishSSHMaster();
    if (!sshSuccess) {
      throw new Error('Failed to establish SSH connection');
    }
    sshEstablished = true;
    
    const localRecord = await testLocalRecord();
    
    if (!localRecord) {
      console.log('\n❌ No local record found for testing');
      return;
    }
    
    const phone = localRecord.caller_id_number || localRecord.callee_id_number;
    await testFormsNew(phone);
    
    if (localRecord.call_id) {
      await testWebhookLogs(localRecord.call_id);
    } else {
      console.log('\n⚠️ No call_id in local record, skipping webhook_logs test');
    }
    
  } catch (error) {
    console.error('\n❌ Fatal error:', error.message);
    console.error(error.stack);
  } finally {
    if (sshEstablished) {
      await closeSSHMaster();
    }
  }
}

main();
