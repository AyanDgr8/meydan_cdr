import mysql from 'mysql2/promise';
import { exec, spawn } from 'child_process';
import { promisify } from 'util';
import path from 'path';
import fs from 'fs';
import dotenv from 'dotenv';

dotenv.config();

const execAsync = promisify(exec);

// Configuration
const LOOKBACK_HOURS = parseInt(process.argv[2]) || 24;
const SSH_CONTROL_PATH = path.join(process.cwd(), '.ssh-control-%r@%h:%p');
const SSH_PASSPHRASE = process.env.SSH_PASSPHRASE || 'avesun123';

// Logging utility
function log(message, level = 'info') {
  const timestamp = new Date().toISOString();
  const prefix = level === 'error' ? '❌ ERROR' : level === 'warning' ? '⚠️ WARNING' : level === 'success' ? '✅ SUCCESS' : 'ℹ️ INFO';
  console.log(`[${timestamp}] ${prefix}: ${message}`);
}

// Normalize phone number by removing non-digits and leading zeros
function normalizePhoneNumber(phone) {
  if (!phone) return '';
  const digits = phone.replace(/\D/g, '');
  return digits.replace(/^0+/, '');
}

// Parse custom date format: DD/MM/YYYY, HH:MM:SS
function parseCustomDate(dateStr) {
  if (!dateStr) return null;
  const match = dateStr.match(/(\d{2})\/(\d{2})\/(\d{4}),?\s*(\d{2}):(\d{2}):(\d{2})/);
  if (!match) return null;
  const [, day, month, year, hour, minute, second] = match;
  return new Date(year, month - 1, day, hour, minute, second);
}

// Fetch UUID from raw_queue_outbound table
async function getUuidFromRawQueue(localConnection, callId) {
  if (!callId) {
    return null;
  }
  
  try {
    const query = `
      SELECT 
        JSON_EXTRACT(raw_data, '$.agent_history') as agent_history
      FROM raw_queue_outbound
      WHERE callid = ?
      LIMIT 1
    `;
    
    const [rows] = await localConnection.execute(query, [callId]);
    
    if (rows.length > 0 && rows[0].agent_history) {
      let agentHistory = rows[0].agent_history;
      
      // Parse if it's a string, otherwise use as-is
      if (typeof agentHistory === 'string') {
        agentHistory = JSON.parse(agentHistory);
      }
      
      // Search through all agent_history entries for agent type with non-empty UUID
      if (Array.isArray(agentHistory)) {
        for (const entry of agentHistory) {
          if (entry.type === 'agent' && entry.uuid && entry.uuid !== '') {
            log(`  Found UUID in raw_queue_outbound: ${entry.uuid}`);
            return entry.uuid;
          }
        }
      }
    }
    
    log(`  No UUID found in raw_queue_outbound for call_id: ${callId}`, 'warning');
    return null;
  } catch (error) {
    log(`  Error fetching UUID from raw_queue_outbound: ${error.message}`, 'error');
    return null;
  }
}

// Establish SSH master connection with automatic passphrase
async function establishSSHMaster() {
  const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
  
  if (!fs.existsSync(pemPath)) {
    throw new Error(`PEM file not found at: ${pemPath}`);
  }
  
  log('Establishing persistent SSH connection with auto-passphrase...');
  
  // Create a temporary askpass script
  const askpassScript = path.join(process.cwd(), '.ssh-askpass.sh');
  const askpassContent = `#!/bin/bash\necho "${SSH_PASSPHRASE}"`;
  
  try {
    fs.writeFileSync(askpassScript, askpassContent, { mode: 0o755 });
  } catch (err) {
    log(`Failed to create askpass script: ${err.message}`, 'error');
    return false;
  }
  
  return new Promise((resolve, reject) => {
    const sshArgs = [
      '-i', pemPath,
      '-o', 'ControlMaster=yes',
      '-o', `ControlPath=${SSH_CONTROL_PATH}`,
      '-o', 'ControlPersist=10m',
      '-o', 'StrictHostKeyChecking=no',
      '-p', '11446',
      'root@94.206.56.70',
      '-N', '-f'
    ];
    
    const env = {
      ...process.env,
      SSH_ASKPASS: askpassScript,
      DISPLAY: ':0',
      SSH_ASKPASS_REQUIRE: 'force'
    };
    
    const sshProcess = spawn('ssh', sshArgs, {
      env: env,
      stdio: ['pipe', 'pipe', 'pipe'],
      detached: false
    });
    
    let errorOutput = '';
    let resolved = false;
    
    sshProcess.stdout.on('data', (data) => {
      log(`SSH stdout: ${data.toString()}`);
    });
    
    sshProcess.stderr.on('data', (data) => {
      const output = data.toString();
      errorOutput += output;
      
      // Check for existing connection
      if (output.includes('Control socket connect')) {
        if (!resolved) {
          resolved = true;
          log('SSH master connection already exists', 'success');
          sshProcess.kill();
          // Clean up askpass script
          try { fs.unlinkSync(askpassScript); } catch (e) {}
          resolve(true);
        }
      }
    });
    
    sshProcess.on('close', (code) => {
      if (!resolved) {
        resolved = true;
        // Clean up askpass script
        try { fs.unlinkSync(askpassScript); } catch (e) {}
        
        if (code === 0) {
          log('SSH master connection established', 'success');
          resolve(true);
        } else {
          log(`Failed to establish SSH connection (code ${code}): ${errorOutput}`, 'error');
          resolve(false);
        }
      }
    });
    
    sshProcess.on('error', (error) => {
      if (!resolved) {
        resolved = true;
        log(`SSH process error: ${error.message}`, 'error');
        // Clean up askpass script
        try { fs.unlinkSync(askpassScript); } catch (e) {}
        resolve(false);
      }
    });
    
    // Timeout after 30 seconds
    setTimeout(() => {
      if (!resolved) {
        resolved = true;
        sshProcess.kill();
        log('SSH connection attempt timed out', 'error');
        // Clean up askpass script
        try { fs.unlinkSync(askpassScript); } catch (e) {}
        resolve(false);
      }
    }, 30000);
  });
}

// Check if SSH master connection is active
async function checkSSHMaster() {
  const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
  const checkCmd = `ssh -i "${pemPath}" -o ControlPath="${SSH_CONTROL_PATH}" -O check -p 11446 root@94.206.56.70 2>&1`;
  
  try {
    await execAsync(checkCmd);
    return true;
  } catch (error) {
    return false;
  }
}

// Ensure SSH master connection is active, re-establish if needed
async function ensureSSHMaster() {
  const isActive = await checkSSHMaster();
  if (!isActive) {
    log('SSH master connection lost, re-establishing...', 'warning');
    return await establishSSHMaster();
  }
  return true;
}

// Close SSH master connection
async function closeSSHMaster() {
  const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
  const closeCmd = `ssh -i "${pemPath}" -o ControlPath="${SSH_CONTROL_PATH}" -O exit -p 11446 root@94.206.56.70 2>/dev/null`;
  
  try {
    await execAsync(closeCmd);
    log('SSH master connection closed');
  } catch (error) {
    // Ignore errors when closing
  }
}

// Connect to local MySQL database
async function connectToLocalDB() {
  log('Connecting to local MySQL database...');
  const connection = await mysql.createConnection({
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || 'Ayan@1012',
    database: process.env.DB_NAME || 'meydan_main_cdr',
    port: parseInt(process.env.DB_PORT) || 3306
  });
  log('Connected to local database', 'success');
  return connection;
}

// Get Outbound records with empty dispositions
async function getEmptyDispositionRecords(connection, hours) {
  log(`Fetching Outbound records from last ${hours} hours with empty dispositions and valid agent_name...`);
  
  const query = `
    SELECT 
      id,
      call_id,
      callee_id_number,
      hangup_time,
      agent_name
    FROM final_report
    WHERE record_type = 'Outbound'
      AND (agent_disposition IS NULL OR agent_disposition = '')
      AND agent_disposition != 'AGENT NOT SUBMITTED'
      AND agent_name IS NOT NULL
      AND agent_name != ''
      AND called_time >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL ? HOUR))
    ORDER BY id DESC
  `;
  
  const [rows] = await connection.execute(query, [hours]);
  log(`Found ${rows.length} Outbound records with empty dispositions and valid agent_name (last ${hours} hours)`);
  
  return rows;
}

// Query webhook_logs table for disposition data from payload
async function queryWebhookLogs(callId) {
  if (!callId) {
    return null;
  }
  
  // Ensure SSH master connection is active
  await ensureSSHMaster();
  
  const sqlQuery = `SELECT payload FROM webhook_logs WHERE call_id = '${callId}' AND payload IS NOT NULL ORDER BY created_at DESC LIMIT 1`;
  
  const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
  
  if (!fs.existsSync(pemPath)) {
    throw new Error(`PEM file not found at: ${pemPath}`);
  }
  
  const escapedQuery = sqlQuery.replace(/'/g, "'\\''");
  const sshCommand = `ssh -i "${pemPath}" -o ControlPath="${SSH_CONTROL_PATH}" -p 11446 root@94.206.56.70 "ssh my02 \\"docker exec mysql mysql -umeydanuser -pAyan@1012 meydanform -e '${escapedQuery}' -s -N\\""`;
  
  try {
    const { stdout, stderr } = await execAsync(sshCommand, {
      timeout: 10000,
      maxBuffer: 1024 * 1024
    });
    
    if (stderr && !stderr.includes('Warning')) {
      log(`  SSH stderr: ${stderr}`, 'warning');
    }
    
    const output = stdout.trim();
    
    if (!output) {
      log(`  No webhook_logs entry found for call_id: ${callId}`, 'warning');
      return null;
    }
    
    try {
      const payload = JSON.parse(output);
      
      log(`  ✓ Found webhook_logs entry for call_id: ${callId}`);
      
      // Convert NULL to empty string and handle "General Enquiry" case
      const cleanValue = (val) => {
        if (!val || val === 'NULL' || val.trim() === '') return '';
        return val;
      };
      
      const processDisposition = (val) => {
        const cleaned = cleanValue(val);
        if (cleaned === 'General Enquiry') return 'GENERAL ENQUIRY';
        return cleaned;
      };
      
      // Extract nested subdisposition values
      const disposition = processDisposition(payload.disposition || '');
      const subDisp1 = cleanValue(payload.subdisposition?.name || '');
      const subDisp2 = cleanValue(payload.subdisposition?.subdisposition?.name || '');
      const subDisp3 = cleanValue(payload.subdisposition?.subdisposition?.subdisposition?.name || '');
      const followUpNotes = cleanValue(payload.follow_up_notes || '');
      
      // If no meaningful disposition data exists, return null
      if (!disposition && !subDisp1 && !subDisp2 && !subDisp3 && !followUpNotes) {
        log(`  No disposition data in webhook_logs payload for call_id: ${callId}`, 'warning');
        return null;
      }
      
      return {
        agent_disposition: disposition,
        sub_disp_1: subDisp1,
        sub_disp_2: subDisp2,
        sub_disp_3: subDisp3,
        follow_up_notes: followUpNotes,
        source: 'webhook_logs'
      };
    } catch (parseError) {
      log(`  Error parsing webhook_logs payload: ${parseError.message}`, 'error');
      return null;
    }
  } catch (error) {
    if (error.killed) {
      log(`Timeout querying webhook_logs for call_id: ${callId}`, 'warning');
    } else {
      log(`Error querying webhook_logs for call_id: ${callId}: ${error.message}`, 'error');
    }
    return null;
  }
}

// Query remote database for disposition data
async function queryRemoteDatabase(calleeIdNumber, hangupTimeStr) {
  const normalizedCallee = normalizePhoneNumber(calleeIdNumber);
  
  if (!normalizedCallee || !hangupTimeStr) {
    return null;
  }
  
  // Ensure SSH master connection is active
  await ensureSSHMaster();
  
  // Parse custom date format: DD/MM/YYYY, HH:MM:SS
  const hangupDate = parseCustomDate(hangupTimeStr);
  
  if (!hangupDate || isNaN(hangupDate.getTime())) {
    return null;
  }
  
  const twoHoursBefore = new Date(hangupDate.getTime() - 2 * 60 * 60 * 1000);
  const twoHoursAfter = new Date(hangupDate.getTime() + 2 * 60 * 60 * 1000);
  
  // Format as local time string (both databases store IST timestamps)
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
  
  const sqlQuery = `SELECT contact_number,call_type,disposition_1,disposition_2,disposition_3,query,created_at FROM forms_new WHERE (REPLACE(REPLACE(REPLACE(contact_number,' ',''),'-',''),'+','') LIKE '%${normalizedCallee}' OR REPLACE(REPLACE(REPLACE(contact_number,' ',''),'-',''),'+','') LIKE '${normalizedCallee}%') AND created_at BETWEEN '${beforeStr}' AND '${afterStr}' ORDER BY ABS(TIMESTAMPDIFF(SECOND,created_at,'${hangupStr}')) ASC LIMIT 1`;
  
  const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
  
  if (!fs.existsSync(pemPath)) {
    throw new Error(`PEM file not found at: ${pemPath}`);
  }
  
  // Escape single quotes in the SQL query for bash
  const escapedQuery = sqlQuery.replace(/'/g, "'\\''");
  const sshCommand = `ssh -i "${pemPath}" -o ControlPath="${SSH_CONTROL_PATH}" -p 11446 root@94.206.56.70 "ssh my02 \\"docker exec mysql mysql -umeydanuser -pAyan@1012 meydanform -e '${escapedQuery}' -s -N\\""`;
  
  try {
    const { stdout, stderr } = await execAsync(sshCommand, {
      timeout: 10000,
      maxBuffer: 1024 * 1024
    });
    
    if (stderr && !stderr.includes('Warning')) {
      log(`  SSH stderr: ${stderr}`, 'warning');
    }
    
    const output = stdout.trim();
    
    if (!output) {
      log(`  No output from remote query for ${calleeIdNumber} (normalized: ${normalizedCallee})`, 'warning');
      log(`  Time window: ${beforeStr} to ${afterStr}`, 'warning');
      return null;
    }
    
    log(`  Raw output: ${output.substring(0, 200)}...`);
    
    const values = output.split('\t');
    
    if (values.length >= 7) {
      const createdAt = new Date(values[6]);
      const timeDiff = Math.abs((createdAt.getTime() - hangupDate.getTime()) / 1000);
      
      log(`  ✓ Match found for ${calleeIdNumber}: time diff ${timeDiff.toFixed(0)}s`);
      
      // Convert NULL to empty string and handle "General Enquiry" case
      const cleanValue = (val) => {
        if (!val || val === 'NULL' || val.trim() === '') return '';
        return val;
      };
      
      const processDisposition = (val) => {
        const cleaned = cleanValue(val);
        // Only uppercase "General Enquiry"
        if (cleaned === 'General Enquiry') return 'GENERAL ENQUIRY';
        return cleaned;
      };
      
      return {
        agent_disposition: processDisposition(values[1]),
        sub_disp_1: cleanValue(values[2]),
        sub_disp_2: cleanValue(values[3]),
        sub_disp_3: cleanValue(values[4]),
        follow_up_notes: cleanValue(values[5]),
        time_difference: timeDiff
      };
    }
    
    return null;
  } catch (error) {
    if (error.killed) {
      log(`Timeout querying remote database for ${calleeIdNumber}`, 'warning');
    } else {
      log(`Error querying remote database for ${calleeIdNumber}: ${error.message}`, 'error');
    }
    return null;
  }
}

// Update final_report with disposition data
async function updateFinalReport(connection, recordId, dispositionData) {
  const query = `
    UPDATE final_report
    SET 
      agent_disposition = ?,
      sub_disp_1 = ?,
      sub_disp_2 = ?,
      sub_disp_3 = ?,
      follow_up_notes = ?
    WHERE id = ?
  `;
  
  const params = [
    dispositionData.agent_disposition || null,
    dispositionData.sub_disp_1 || null,
    dispositionData.sub_disp_2 || null,
    dispositionData.sub_disp_3 || null,
    dispositionData.follow_up_notes || null,
    recordId
  ];
  
  await connection.execute(query, params);
}

async function fillEmptyDispositions() {
  const startTime = Date.now();
  log('========================================');
  log('Starting Fill Empty Dispositions Process (OUTBOUND)');
  log(`Lookback Period: ${LOOKBACK_HOURS} hours`);
  log('========================================');
  
  let localConnection = null;
  let updatedCount = 0;
  let notFoundCount = 0;
  let errorCount = 0;
  let sshEstablished = false;
  const updatedRecords = [];
  
  try {
    const sshSuccess = await establishSSHMaster();
    if (!sshSuccess) {
      throw new Error('Failed to establish SSH connection');
    }
    sshEstablished = true;
    
    localConnection = await connectToLocalDB();
    
    const emptyRecords = await getEmptyDispositionRecords(localConnection, LOOKBACK_HOURS);
    
    if (emptyRecords.length === 0) {
      log('No records with empty dispositions found in the specified time period', 'success');
      return;
    }
    
    log(`Processing ${emptyRecords.length} records...`);
    
    for (const record of emptyRecords) {
      try {
        log(`\nProcessing record ID ${record.id} (${record.callee_id_number})...`);
        
        // First try forms_new table
        let dispositionData = await queryRemoteDatabase(
          record.callee_id_number,
          record.hangup_time
        );
        
        // If not found in forms_new, try webhook_logs with call_id
        if (!dispositionData && record.call_id) {
          log(`  Trying webhook_logs for call_id: ${record.call_id}...`);
          dispositionData = await queryWebhookLogs(record.call_id);
          
          // If still not found, try with UUID from raw_queue_outbound
          if (!dispositionData) {
            log(`  Fetching UUID from raw_queue_outbound...`);
            const uuid = await getUuidFromRawQueue(localConnection, record.call_id);
            
            if (uuid) {
              log(`  Trying webhook_logs for UUID: ${uuid}...`);
              dispositionData = await queryWebhookLogs(uuid);
            }
          }
        }
        
        if (dispositionData) {
          await updateFinalReport(localConnection, record.id, dispositionData);
          
          updatedCount++;
          const source = dispositionData.source || 'forms_new';
          updatedRecords.push({
            id: record.id,
            phone: record.callee_id_number,
            disposition: dispositionData.agent_disposition,
            sub1: dispositionData.sub_disp_1,
            sub2: dispositionData.sub_disp_2,
            source: source
          });
          
          log(`  ✅ Updated record ID ${record.id} (source: ${source}):`, 'success');
          if (dispositionData.agent_disposition) log(`     - agent_disposition: ${dispositionData.agent_disposition}`);
          if (dispositionData.sub_disp_1) log(`     - sub_disp_1: ${dispositionData.sub_disp_1}`);
          if (dispositionData.sub_disp_2) log(`     - sub_disp_2: ${dispositionData.sub_disp_2}`);
          if (dispositionData.sub_disp_3) log(`     - sub_disp_3: ${dispositionData.sub_disp_3}`);
          if (dispositionData.follow_up_notes) log(`     - follow_up_notes: ${dispositionData.follow_up_notes}`);
        } else {
          // Insert "AGENT NOT SUBMITTED" when no disposition found
          const notSubmittedData = {
            agent_disposition: 'AGENT NOT SUBMITTED',
            sub_disp_1: '',
            sub_disp_2: '',
            sub_disp_3: '',
            follow_up_notes: ''
          };
          await updateFinalReport(localConnection, record.id, notSubmittedData);
          
          notFoundCount++;
          log(`  ⚠️ Marked as AGENT NOT SUBMITTED for record ID ${record.id}`, 'warning');
        }
        
      } catch (error) {
        errorCount++;
        log(`Error processing record ID ${record.id}: ${error.message}`, 'error');
      }
    }
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    
    log('\n========================================');
    log('Process Completed', 'success');
    log('========================================');
    log(`Total records processed: ${emptyRecords.length}`);
    log(`Successfully updated: ${updatedCount}`);
    log(`No match found: ${notFoundCount}`);
    log(`Errors: ${errorCount}`);
    log(`Duration: ${duration}s`);
    log('========================================');
    
    if (updatedRecords.length > 0) {
      log('\n📋 Updated Records Summary:');
      log('========================================');
      updatedRecords.forEach((rec, index) => {
        log(`${index + 1}. Record ID: ${rec.id} [${rec.source}]`);
        log(`   Phone: ${rec.phone}`);
        log(`   Disposition: ${rec.disposition} > ${rec.sub1} > ${rec.sub2}`);
      });
      log('========================================');
    }
    
  } catch (error) {
    log(`Fatal error: ${error.message}`, 'error');
    log(error.stack, 'error');
    throw error;
  } finally {
    if (localConnection) {
      await localConnection.end();
      log('Local database connection closed');
    }
    
    if (sshEstablished) {
      await closeSSHMaster();
    }
  }
}

// Main execution
fillEmptyDispositions()
  .then(() => {
    log('Script completed successfully', 'success');
    process.exit(0);
  })
  .catch((error) => {
    log(`Script failed: ${error.message}`, 'error');
    process.exit(1);
  });
