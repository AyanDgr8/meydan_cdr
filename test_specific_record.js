import mysql from 'mysql2/promise';
import { exec } from 'child_process';
import { promisify } from 'util';
import path from 'path';
import fs from 'fs';
import dotenv from 'dotenv';

dotenv.config();

const execAsync = promisify(exec);
const SSH_CONTROL_PATH = path.join(process.cwd(), '.ssh-control-%r@%h:%p');

function parseCustomDate(dateStr) {
  if (!dateStr) return null;
  const match = dateStr.match(/(\d{2})\/(\d{2})\/(\d{4}),?\s*(\d{2}):(\d{2}):(\d{2})/);
  if (!match) return null;
  const [, day, month, year, hour, minute, second] = match;
  return new Date(year, month - 1, day, hour, minute, second);
}

function normalizePhoneNumber(phone) {
  if (!phone) return '';
  const digits = phone.replace(/\D/g, '');
  return digits.replace(/^0+/, '');
}

async function establishSSHMaster() {
  const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
  const sshMasterCmd = `ssh -i "${pemPath}" -o ControlMaster=yes -o ControlPath="${SSH_CONTROL_PATH}" -o ControlPersist=10m -p 11446 root@94.206.56.70 -N -f`;
  
  try {
    await execAsync(sshMasterCmd);
    console.log('✅ SSH established');
    return true;
  } catch (error) {
    if (error.message.includes('Control socket connect')) {
      console.log('✅ SSH already exists');
      return true;
    }
    return false;
  }
}

async function closeSSHMaster() {
  const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
  const closeCmd = `ssh -i "${pemPath}" -o ControlPath="${SSH_CONTROL_PATH}" -O exit -p 11446 root@94.206.56.70 2>/dev/null`;
  try {
    await execAsync(closeCmd);
  } catch (error) {}
}

async function testQuery() {
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'Ayan@1012',
    database: 'meydan_main_cdr',
    port: 3306
  });
  
  // Get the specific record
  const [rows] = await connection.execute(
    `SELECT id, call_id, caller_id_number, callee_id_number, hangup_time 
     FROM final_report 
     WHERE id = 660489`
  );
  
  const record = rows[0];
  console.log('\n=== LOCAL RECORD ===');
  console.log(`ID: ${record.id}`);
  console.log(`Call ID: ${record.call_id}`);
  console.log(`Caller: ${record.caller_id_number}`);
  console.log(`Callee: ${record.callee_id_number}`);
  console.log(`Hangup Time: ${record.hangup_time}`);
  
  await connection.end();
  
  // Parse and format time
  const hangupDate = parseCustomDate(record.hangup_time);
  console.log(`\nParsed Hangup Date: ${hangupDate}`);
  
  const twoHoursBefore = new Date(hangupDate.getTime() - 2 * 60 * 60 * 1000);
  const twoHoursAfter = new Date(hangupDate.getTime() + 2 * 60 * 60 * 1000);
  
  const formatLocalTime = (date) => {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  };
  
  const beforeStr = formatLocalTime(twoHoursBefore);
  const afterStr = formatLocalTime(twoHoursAfter);
  const hangupStr = formatLocalTime(hangupDate);
  
  console.log(`\n=== TIME WINDOW (LOCAL IST) ===`);
  console.log(`Before: ${beforeStr}`);
  console.log(`Hangup: ${hangupStr}`);
  console.log(`After:  ${afterStr}`);
  
  // Query forms_new
  const normalizedCallee = normalizePhoneNumber(record.callee_id_number);
  console.log(`\nNormalized Callee: ${normalizedCallee}`);
  
  const sqlQuery = `SELECT contact_number,call_type,disposition_1,disposition_2,disposition_3,query,created_at FROM forms_new WHERE (REPLACE(REPLACE(REPLACE(contact_number,' ',''),'-',''),'+','') LIKE '%${normalizedCallee}' OR REPLACE(REPLACE(REPLACE(contact_number,' ',''),'-',''),'+','') LIKE '${normalizedCallee}%') AND created_at BETWEEN '${beforeStr}' AND '${afterStr}' ORDER BY ABS(TIMESTAMPDIFF(SECOND,created_at,'${hangupStr}')) ASC LIMIT 1`;
  
  console.log(`\n=== SQL QUERY ===`);
  console.log(sqlQuery);
  
  const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
  const escapedQuery = sqlQuery.replace(/'/g, "'\\''");
  const sshCommand = `ssh -i "${pemPath}" -o ControlPath="${SSH_CONTROL_PATH}" -p 11446 root@94.206.56.70 "ssh my02 \\"docker exec mysql mysql -umeydanuser -pAyan@1012 meydanform -e '${escapedQuery}' -s -N\\""`;
  
  console.log(`\n=== EXECUTING QUERY ===`);
  
  try {
    const { stdout, stderr } = await execAsync(sshCommand, {
      timeout: 10000,
      maxBuffer: 1024 * 1024
    });
    
    const output = stdout.trim();
    
    if (!output) {
      console.log('❌ No results found');
    } else {
      console.log('✅ FOUND MATCH:');
      console.log(output);
      
      const values = output.split('\t');
      console.log(`\nParsed:`);
      console.log(`  Contact: ${values[0]}`);
      console.log(`  Call Type: ${values[1]}`);
      console.log(`  Disposition 1: ${values[2]}`);
      console.log(`  Disposition 2: ${values[3]}`);
      console.log(`  Disposition 3: ${values[4]}`);
      console.log(`  Query: ${values[5]}`);
      console.log(`  Created At: ${values[6]}`);
    }
  } catch (error) {
    console.error('❌ Error:', error.message);
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
    
    await testQuery();
    
  } catch (error) {
    console.error('\n❌ Fatal error:', error.message);
  } finally {
    if (sshEstablished) {
      await closeSSHMaster();
    }
  }
}

main();
