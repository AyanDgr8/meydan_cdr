import mysql from 'mysql2/promise';
import { Client } from 'ssh2';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';

dotenv.config();

const LOG_FILE = path.join(process.cwd(), 'fill-dispositions.log');
const SSH_PASSPHRASE = process.env.SSH_PASSPHRASE || 'avesun123';

function log(message, type = 'info') {
  const timestamp = new Date().toISOString();
  let formattedMessage;
  
  switch (type) {
    case 'error':
      formattedMessage = `[${timestamp}] ❌ ERROR: ${message}`;
      console.error(formattedMessage);
      break;
    case 'warning':
      formattedMessage = `[${timestamp}] ⚠️ WARNING: ${message}`;
      console.warn(formattedMessage);
      break;
    case 'success':
      formattedMessage = `[${timestamp}] ✅ SUCCESS: ${message}`;
      console.log(formattedMessage);
      break;
    default:
      formattedMessage = `[${timestamp}] ℹ️ INFO: ${message}`;
      console.log(formattedMessage);
  }
  
  fs.appendFileSync(LOG_FILE, formattedMessage + '\n');
  return formattedMessage;
}

function normalizePhoneNumber(phoneNumber) {
  if (!phoneNumber || typeof phoneNumber !== 'string') {
    return '';
  }
  
  let normalized = phoneNumber.replace(/\D/g, '');
  normalized = normalized.replace(/^0+/, '');
  
  return normalized;
}

async function createSSHTunnel() {
  return new Promise((resolve, reject) => {
    const sshClient = new Client();
    
    const pemPath = path.join(process.cwd(), 'public', 'uploads', 'aveek.pem');
    
    if (!fs.existsSync(pemPath)) {
      return reject(new Error(`PEM file not found at: ${pemPath}`));
    }
    
    log('Reading PEM file...');
    const privateKey = fs.readFileSync(pemPath);
    
    log('Establishing SSH connection to 94.206.56.70:11446...');
    
    sshClient.on('ready', () => {
      log('SSH connection established', 'success');
      
      sshClient.exec('ssh my02', (err, stream) => {
        if (err) {
          sshClient.end();
          return reject(err);
        }
        
        log('Executing ssh my02...');
        
        stream.on('close', (code, signal) => {
          log(`SSH my02 stream closed with code ${code}`);
        }).on('data', (data) => {
          log(`SSH my02 output: ${data}`);
        }).stderr.on('data', (data) => {
          log(`SSH my02 stderr: ${data}`, 'warning');
        });
        
        log('Setting up port forwarding for MySQL...');
        
        sshClient.forwardOut(
          '127.0.0.1',
          0,
          '127.0.0.1',
          3306,
          (err, stream) => {
            if (err) {
              sshClient.end();
              return reject(err);
            }
            
            log('Port forwarding established', 'success');
            resolve({ sshClient, stream });
          }
        );
      });
    });
    
    sshClient.on('error', (err) => {
      log(`SSH connection error: ${err.message}`, 'error');
      reject(err);
    });
    
    sshClient.connect({
      host: '94.206.56.70',
      port: 11446,
      username: 'root',
      privateKey: privateKey,
      passphrase: SSH_PASSPHRASE,
      readyTimeout: 30000
    });
  });
}

async function connectToRemoteDB(stream) {
  log('Connecting to remote MySQL database...');
  
  const connection = await mysql.createConnection({
    user: 'meydanuser',
    password: 'Ayan@1012',
    database: 'meydanform',
    stream: stream
  });
  
  log('Connected to remote database', 'success');
  return connection;
}

async function connectToLocalDB() {
  log('Connecting to local MySQL database...');
  
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'Ayan@1012',
    database: 'meydan_main_cdr',
    port: 3306
  });
  
  log('Connected to local database', 'success');
  return connection;
}

async function getEmptyDispositionRecords(localConnection) {
  log('Fetching records with empty dispositions from final_report...');
  
  const query = `
    SELECT 
      id,
      call_id,
      caller_id_number,
      hangup_time,
      record_type
    FROM final_report
    WHERE record_type = 'Inbound'
      AND (agent_disposition IS NULL OR agent_disposition = '')
      AND (sub_disp_1 IS NULL OR sub_disp_1 = '')
      AND (sub_disp_2 IS NULL OR sub_disp_2 = '')
      AND (sub_disp_3 IS NULL OR sub_disp_3 = '')
      AND (follow_up_notes IS NULL OR follow_up_notes = '')
    ORDER BY hangup_time DESC
  `;
  
  const [rows] = await localConnection.execute(query);
  log(`Found ${rows.length} records with empty dispositions`);
  
  return rows;
}

async function findMatchingDisposition(remoteConnection, callerIdNumber, hangupTime) {
  const normalizedCaller = normalizePhoneNumber(callerIdNumber);
  
  if (!normalizedCaller || !hangupTime) {
    return null;
  }
  
  const hangupTimestamp = typeof hangupTime === 'bigint' ? Number(hangupTime) : hangupTime;
  
  if (!hangupTimestamp || hangupTimestamp <= 0 || isNaN(hangupTimestamp)) {
    return null;
  }
  
  const hangupDate = new Date(hangupTimestamp * 1000);
  
  if (isNaN(hangupDate.getTime())) {
    return null;
  }
  
  const fifteenMinutesBefore = new Date(hangupDate.getTime() - 15 * 60 * 1000);
  const fifteenMinutesAfter = new Date(hangupDate.getTime() + 15 * 60 * 1000);
  
  const query = `
    SELECT 
      contact_number,
      call_type,
      disposition_1,
      disposition_2,
      disposition_3,
      query,
      created_at
    FROM forms_new
    WHERE (
      REPLACE(REPLACE(REPLACE(contact_number, ' ', ''), '-', ''), '+', '') LIKE ?
      OR REPLACE(REPLACE(REPLACE(contact_number, ' ', ''), '-', ''), '+', '') LIKE ?
    )
    AND created_at BETWEEN ? AND ?
    ORDER BY ABS(TIMESTAMPDIFF(SECOND, created_at, ?)) ASC
    LIMIT 1
  `;
  
  const params = [
    `%${normalizedCaller}`,
    `${normalizedCaller}%`,
    fifteenMinutesBefore,
    fifteenMinutesAfter,
    hangupDate
  ];
  
  try {
    const [rows] = await remoteConnection.execute(query, params);
    
    if (rows.length > 0) {
      const match = rows[0];
      const timeDiff = Math.abs(
        (new Date(match.created_at).getTime() - hangupDate.getTime()) / 1000
      );
      
      log(`  ✓ Match found for ${callerIdNumber}: time diff ${timeDiff.toFixed(0)}s`);
      
      return {
        agent_disposition: match.call_type || null,
        sub_disp_1: match.disposition_1 || null,
        sub_disp_2: match.disposition_2 || null,
        sub_disp_3: match.disposition_3 || null,
        follow_up_notes: match.query || null,
        time_difference: timeDiff
      };
    }
    
    return null;
  } catch (error) {
    log(`Error finding match for ${callerIdNumber}: ${error.message}`, 'error');
    return null;
  }
}

async function updateFinalReport(localConnection, recordId, dispositionData) {
  const query = `
    UPDATE final_report
    SET 
      agent_disposition = ?,
      sub_disp_1 = ?,
      sub_disp_2 = ?,
      sub_disp_3 = ?,
      follow_up_notes = ?,
      updated_at = NOW()
    WHERE id = ?
  `;
  
  const params = [
    dispositionData.agent_disposition,
    dispositionData.sub_disp_1,
    dispositionData.sub_disp_2,
    dispositionData.sub_disp_3,
    dispositionData.follow_up_notes,
    recordId
  ];
  
  await localConnection.execute(query, params);
}

async function fillEmptyDispositions() {
  const startTime = Date.now();
  log('========================================');
  log('Starting Fill Empty Dispositions Process');
  log('========================================');
  
  let sshClient = null;
  let remoteConnection = null;
  let localConnection = null;
  let updatedCount = 0;
  let notFoundCount = 0;
  
  try {
    localConnection = await connectToLocalDB();
    
    const emptyRecords = await getEmptyDispositionRecords(localConnection);
    
    if (emptyRecords.length === 0) {
      log('No records with empty dispositions found', 'success');
      return;
    }
    
    const { sshClient: client, stream } = await createSSHTunnel();
    sshClient = client;
    
    remoteConnection = await connectToRemoteDB(stream);
    
    log(`Processing ${emptyRecords.length} records...`);
    
    for (const record of emptyRecords) {
      try {
        log(`\nProcessing record ID ${record.id} (${record.caller_id_number})...`);
        
        const dispositionData = await findMatchingDisposition(
          remoteConnection,
          record.caller_id_number,
          record.hangup_time
        );
        
        if (dispositionData) {
          await updateFinalReport(localConnection, record.id, dispositionData);
          
          updatedCount++;
          log(`  ✅ Updated record ID ${record.id}:`, 'success');
          log(`     - agent_disposition: ${dispositionData.agent_disposition}`);
          log(`     - sub_disp_1: ${dispositionData.sub_disp_1}`);
          log(`     - sub_disp_2: ${dispositionData.sub_disp_2}`);
          log(`     - sub_disp_3: ${dispositionData.sub_disp_3}`);
          log(`     - follow_up_notes: ${dispositionData.follow_up_notes}`);
        } else {
          notFoundCount++;
          log(`  ⚠️ No matching disposition found for record ID ${record.id}`, 'warning');
        }
        
      } catch (error) {
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
    log(`Duration: ${duration}s`);
    log('========================================');
    
  } catch (error) {
    log(`Fatal error: ${error.message}`, 'error');
    log(error.stack, 'error');
    throw error;
  } finally {
    if (remoteConnection) {
      await remoteConnection.end();
      log('Remote database connection closed');
    }
    
    if (localConnection) {
      await localConnection.end();
      log('Local database connection closed');
    }
    
    if (sshClient) {
      sshClient.end();
      log('SSH connection closed');
    }
  }
}

fillEmptyDispositions()
  .then(() => {
    log('Script completed successfully', 'success');
    process.exit(0);
  })
  .catch((error) => {
    log(`Script failed: ${error.message}`, 'error');
    process.exit(1);
  });
