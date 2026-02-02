#!/usr/bin/env node

import readline from 'readline';
import dbService from './dbService.js';

// Queue to callee extension mapping
const queueToCalleeExtensionMap = {
  '8000': '7020',
  '8001': '7014',
  '8002': '7015',
  '8003': '7016',
  '8004': '7012',
  '8005': '7017',
  '8006': '7018',
  '8007': '7019',
  '8008': '7021',
  '8009': '7034',
  '8010': '7023',
  '8011': '7028',
  '8012': '7029',
  '8013': '7031',
  '8014': '7033',
  '8015': '7030',
  '8016': '7013',
  '8017': '7011',
  '8018': '7010',
  '8019': '7008'
};

// Transfer detection function - handles both JSON and HTML agent history
function detectTransferEvents(callId, agentHistory, callType) {
  if (!agentHistory) {
    return { transfer_event: false };
  }

  let history = [];
  
  // Handle if it's already an array
  if (Array.isArray(agentHistory)) {
    history = agentHistory;
  }
  // Try to parse as JSON string
  else if (typeof agentHistory === 'string' && (agentHistory.startsWith('[') || agentHistory.startsWith('{'))) {
    try {
      history = JSON.parse(agentHistory);
    } catch (e) {
      return { transfer_event: false };
    }
  } 
  // Parse HTML table format
  else if (agentHistory.includes('<table')) {
    try {
      // Extract table rows using regex
      const rowMatches = agentHistory.match(/<tr><td>([^<]+)<\/td><td>([^<]*)<\/td><td>([^<]*)<\/td><td>([^<]+)<\/td><td>([^<]+)<\/td><td>([^<]*)<\/td><\/tr>/g);
      
      if (rowMatches) {
        history = rowMatches.map(row => {
          const cells = row.match(/<td>([^<]*)<\/td>/g);
          if (cells && cells.length >= 5) {
            const timeStr = cells[0].replace(/<\/?td>/g, '');
            const firstName = cells[1].replace(/<\/?td>/g, '');
            const lastName = cells[2].replace(/<\/?td>/g, '');
            const ext = cells[3].replace(/<\/?td>/g, '');
            const eventType = cells[4].replace(/<\/?td>/g, '');
            
            // Convert time string to timestamp
            let timestamp = 0;
            try {
              const [datePart, timePart] = timeStr.split(', ');
              const [day, month, year] = datePart.split('/');
              const [hour, minute, second] = timePart.split(':');
              const date = new Date(year, month - 1, day, hour, minute, second);
              timestamp = Math.floor(date.getTime() / 1000);
            } catch (e) {
              // Keep timestamp as 0 if parsing fails
            }
            
            return {
              ext: ext,
              type: eventType,
              first_name: firstName,
              last_name: lastName,
              last_attempt: timestamp
            };
          }
          return null;
        }).filter(Boolean);
      }
    } catch (e) {
      console.log(`   ⚠️ Error parsing HTML history: ${e.message}`);
      return { transfer_event: false };
    }
  }

  if (!Array.isArray(history) || history.length === 0) {
    return { transfer_event: false };
  }

  // Look for transfer events to queue extensions (4-digit numbers starting with 8)
  const transferEvents = history.filter(event => {
    const ext = String(event.ext || '');
    return (event.type === 'Transfer' || event.type === 'transfer') && 
           ext.match(/^8\d{3}$/);
  });

  if (transferEvents.length === 0) {
    return { transfer_event: false };
  }

  // Get the last transfer event
  const lastTransferEvent = transferEvents[transferEvents.length - 1];
  
  return {
    transfer_event: true,
    transfer_extension: lastTransferEvent.ext,
    transfer_queue_extension: lastTransferEvent.ext,
    transfer_type: 'campaign_transfer',
    transfer_timestamp: lastTransferEvent.last_attempt
  };
}

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

function formatTimestamp(timestamp) {
  if (!timestamp) return 'N/A';
  return new Date(timestamp * 1000).toISOString().replace('T', ' ').replace('.000Z', '');
}

// Helper function to display agent history
function displayAgentHistory(agentHistory) {
  if (!agentHistory) return;
  
  try {
    const history = typeof agentHistory === 'string' ? JSON.parse(agentHistory) : agentHistory;
    console.log(`   📋 Agent History Events:`);
    history.forEach((event, index) => {
      console.log(`   ${index + 1}. ${event.type} - ${event.ext} - ${formatTimestamp(event.last_attempt)}`);
    });
    
    // Check for any transfer-like events
    const transferLikeEvents = history.filter(event => 
      event.type && (event.type.toLowerCase().includes('transfer') || 
      String(event.ext || '').match(/^8\d{3}$/))
    );
    
    if (transferLikeEvents.length > 0) {
      console.log(`   🔍 Transfer-like events found:`);
      transferLikeEvents.forEach((event, index) => {
        console.log(`   ${index + 1}. ${event.type} - ${event.ext} - ${formatTimestamp(event.last_attempt)}`);
      });
    }
  } catch (e) {
    console.log(`   ⚠️ Could not parse agent history: ${e.message}`);
  }
}

// Helper function to search for inbound matches
async function searchForInboundMatches(transferResult) {
  console.log(`\n🔍 SEARCHING FOR MATCHING INBOUND CALLS:`);
  
  const inboundQuery = `
    SELECT callid, agent_name, extension, called_time, called_time_formatted, 
           callee_id_number, agent_history
    FROM raw_queue_inbound 
    WHERE called_time BETWEEN ? AND ?
      AND callee_id_number = ?
    ORDER BY called_time ASC
  `;
  
  const expectedCalleeId = queueToCalleeExtensionMap[transferResult.transfer_queue_extension];
  const timeWindow = 3600; // 1 hour window
  const startTime = transferResult.transfer_timestamp - timeWindow;
  const endTime = transferResult.transfer_timestamp + timeWindow;
  
  const inboundResults = await dbService.query(inboundQuery, [startTime, endTime, expectedCalleeId]);
  
  if (inboundResults.length > 0) {
    console.log(`   📋 Found ${inboundResults.length} potential matches with callee_id ${expectedCalleeId}:`);
    
    let bestMatch = null;
    let bestTimeDiff = Infinity;
    
    inboundResults.forEach((inbound, index) => {
      const timeDiff = Math.abs(inbound.called_time - transferResult.transfer_timestamp);
      const isClosest = timeDiff < bestTimeDiff;
      
      if (isClosest) {
        bestMatch = inbound;
        bestTimeDiff = timeDiff;
      }
      
      console.log(`   ${index + 1}. ${inbound.callid}`);
      console.log(`      👤 Agent: ${inbound.agent_name} (${inbound.extension})`);
      console.log(`      ⏰ Time: ${inbound.called_time_formatted || formatTimestamp(inbound.called_time)}`);
      console.log(`      🕐 Time Diff: ${timeDiff}s ${isClosest ? '⭐ CLOSEST MATCH' : ''}`);
      console.log(`      📞 Callee ID: ${inbound.callee_id_number}`);
    });
    
    if (bestMatch) {
      console.log(`\n🎯 BEST MATCH ANALYSIS:`);
      console.log(`   📞 Inbound Call: ${bestMatch.callid}`);
      console.log(`   👤 Receiving Agent: ${bestMatch.agent_name} (${bestMatch.extension})`);
      console.log(`   ⏰ Time Difference: ${bestTimeDiff} seconds`);
      console.log(`   ✅ Status: ${bestTimeDiff <= 1800 ? 'LINKABLE (within 30 min)' : 'TOO FAR (>30 min)'}`);
    }
  } else {
    console.log(`   ❌ No inbound calls found with callee_id ${expectedCalleeId} in time window`);
    
    // Check if there are any inbound calls with different callee_id
    const anyInboundQuery = `
      SELECT callid, callee_id_number, called_time_formatted, called_time
      FROM raw_queue_inbound 
      WHERE called_time BETWEEN ? AND ?
      ORDER BY called_time ASC
      LIMIT 5
    `;
    
    const anyInboundResults = await dbService.query(anyInboundQuery, [startTime, endTime]);
    
    if (anyInboundResults.length > 0) {
      console.log(`   📋 Found ${anyInboundResults.length} inbound calls in time window with different callee_ids:`);
      anyInboundResults.forEach((inbound, index) => {
        console.log(`   ${index + 1}. ${inbound.callid} - callee_id: ${inbound.callee_id_number} - ${inbound.called_time_formatted || formatTimestamp(inbound.called_time)}`);
      });
      console.log(`   💡 This suggests a queue mapping issue: queue ${transferResult.transfer_queue_extension} should map to a different callee_id`);
    }
  }
}

// Helper function to search for campaign matches
async function searchForCampaignMatches(mappedQueue, inboundCall) {
  console.log(`🔄 MAPPED QUEUE: ${mappedQueue} -> ${inboundCall.callee_id_number}`);
  console.log(`\n🔍 SEARCHING FOR CAMPAIGN CALLS THAT TRANSFERRED TO QUEUE ${mappedQueue}:`);
  
  const campaignQuery = `
    SELECT call_id, agent_name, extension, called_time, called_time_formatted, 
           agent_history, queue_campaign_name
    FROM raw_campaigns 
    WHERE called_time BETWEEN ? AND ?
    ORDER BY called_time DESC
  `;
  
  const timeWindow = 3600; // 1 hour window
  const startTime = inboundCall.called_time - timeWindow;
  const endTime = inboundCall.called_time + timeWindow;
  
  const campaignResults = await dbService.query(campaignQuery, [startTime, endTime]);
  
  let foundMatches = 0;
  
  for (const campaign of campaignResults) {
    const transferResult = detectTransferEvents(campaign.call_id, campaign.agent_history, 'campaign');
    
    if (transferResult.transfer_event && transferResult.transfer_queue_extension === mappedQueue) {
      foundMatches++;
      const timeDiff = Math.abs(campaign.called_time - inboundCall.called_time);
      
      console.log(`   ${foundMatches}. ${campaign.call_id}`);
      console.log(`      👤 Agent: ${campaign.agent_name} (${campaign.extension})`);
      console.log(`      🎯 Campaign: ${campaign.queue_campaign_name}`);
      console.log(`      ⏰ Time: ${campaign.called_time_formatted || formatTimestamp(campaign.called_time)}`);
      console.log(`      🕐 Time Diff: ${timeDiff}s`);
      console.log(`      ✅ Status: ${timeDiff <= 1800 ? 'LINKABLE (within 30 min)' : 'TOO FAR (>30 min)'}`);
    }
  }
  
  if (foundMatches === 0) {
    console.log(`   ❌ No campaign calls found that transferred to queue ${mappedQueue}`);
  }
}

async function analyzeCall(callId) {
  try {
    console.log(`\n🔍 BLA CALL ANALYZER: Checking call ${callId}`);
    console.log('=' .repeat(80));

    // First, check if it's a campaign call in raw_campaigns table
    const campaignQuery = `
      SELECT call_id, campaign_name, timestamp, raw_data
      FROM raw_campaigns 
      WHERE call_id = ?
    `;
    
    const campaignResults = await dbService.query(campaignQuery, [callId]);
    
    if (campaignResults.length > 0) {
      const call = campaignResults[0];
      const rawData = typeof call.raw_data === 'string' ? JSON.parse(call.raw_data) : call.raw_data;
      
      console.log(`📞 CALL TYPE: Campaign Call`);
      console.log(`👤 AGENT: ${rawData.agent_name || 'Unknown'} (${rawData.extension || 'Unknown'})`);
      console.log(`📅 CALL TIME: ${rawData.called_time_formatted || formatTimestamp(call.timestamp)}`);
      console.log(`🎯 CAMPAIGN: ${call.campaign_name}`);
      
      // Check for transfer events - campaigns use lead_history
      const transferResult = detectTransferEvents(call.call_id, rawData.lead_history, 'campaign');
      
      if (transferResult.transfer_event) {
        console.log(`\n✅ BLA TRANSFER DETECTED:`);
        console.log(`   🔄 Transfer Queue: ${transferResult.transfer_queue_extension}`);
        console.log(`   ⏰ Transfer Time: ${formatTimestamp(transferResult.transfer_timestamp)}`);
        console.log(`   📍 Expected Callee ID: ${queueToCalleeExtensionMap[transferResult.transfer_queue_extension] || 'Unknown'}`);
        
        // Now find potential inbound matches
        console.log(`\n🔍 SEARCHING FOR MATCHING INBOUND CALLS:`);
        
        const inboundQuery = `
          SELECT call_id, agent_name, extension, called_time, called_time_formatted, 
                 callee_id_number, agent_history
          FROM final_report 
          WHERE record_type = 'Inbound' 
            AND called_time BETWEEN ? AND ?
            AND callee_id_number = ?
          ORDER BY called_time ASC
        `;
        
        const expectedCalleeId = queueToCalleeExtensionMap[transferResult.transfer_queue_extension];
        const timeWindow = 3600; // 1 hour window
        const startTime = transferResult.transfer_timestamp - timeWindow;
        const endTime = transferResult.transfer_timestamp + timeWindow;
        
        const inboundResults = await dbService.query(inboundQuery, [startTime, endTime, expectedCalleeId]);
        
        if (inboundResults.length > 0) {
          console.log(`   📋 Found ${inboundResults.length} potential matches with callee_id ${expectedCalleeId}:`);
          
          let bestMatch = null;
          let bestTimeDiff = Infinity;
          
          inboundResults.forEach((inbound, index) => {
            const timeDiff = Math.abs(inbound.called_time - transferResult.transfer_timestamp);
            const isClosest = timeDiff < bestTimeDiff;
            
            if (isClosest) {
              bestMatch = inbound;
              bestTimeDiff = timeDiff;
            }
            
            console.log(`   ${index + 1}. ${inbound.call_id}`);
            console.log(`      👤 Agent: ${inbound.agent_name} (${inbound.extension})`);
            console.log(`      ⏰ Time: ${inbound.called_time_formatted}`);
            console.log(`      🕐 Time Diff: ${timeDiff}s ${isClosest ? '⭐ CLOSEST MATCH' : ''}`);
            console.log(`      📞 Callee ID: ${inbound.callee_id_number}`);
          });
          
          if (bestMatch) {
            console.log(`\n🎯 BEST MATCH ANALYSIS:`);
            console.log(`   📞 Inbound Call: ${bestMatch.call_id}`);
            console.log(`   👤 Receiving Agent: ${bestMatch.agent_name} (${bestMatch.extension})`);
            console.log(`   ⏰ Time Difference: ${bestTimeDiff} seconds`);
            console.log(`   ✅ Status: ${bestTimeDiff <= 1800 ? 'LINKABLE (within 30 min)' : 'TOO FAR (>30 min)'}`);
          }
        } else {
          console.log(`   ❌ No inbound calls found with callee_id ${expectedCalleeId} in time window`);
          
          // Check if there are any inbound calls with different callee_id
          const anyInboundQuery = `
            SELECT call_id, callee_id_number, called_time_formatted
            FROM final_report 
            WHERE record_type = 'Inbound' 
              AND called_time BETWEEN ? AND ?
            ORDER BY called_time ASC
            LIMIT 5
          `;
          
          const anyInboundResults = await dbService.query(anyInboundQuery, [startTime, endTime]);
          
          if (anyInboundResults.length > 0) {
            console.log(`   📋 Found ${anyInboundResults.length} inbound calls in time window with different callee_ids:`);
            anyInboundResults.forEach((inbound, index) => {
              console.log(`   ${index + 1}. ${inbound.call_id} - callee_id: ${inbound.callee_id_number} - ${inbound.called_time_formatted}`);
            });
            console.log(`   💡 This suggests a queue mapping issue: queue ${transferResult.transfer_queue_extension} should map to a different callee_id`);
          }
        }
      } else {
        console.log(`\n❌ NO BLA TRANSFER DETECTED`);
        console.log(`   This campaign call does not have transfer events to queue extensions`);
        
        if (call.agent_history) {
          try {
            const history = JSON.parse(call.agent_history);
            console.log(`   📋 Agent History Events:`);
            history.forEach((event, index) => {
              console.log(`   ${index + 1}. ${event.type} - ${event.ext} - ${formatTimestamp(event.last_attempt)}`);
            });
            
            // Check for any transfer-like events
            const transferLikeEvents = history.filter(event => 
              event.type && (event.type.toLowerCase().includes('transfer') || 
              String(event.ext || '').match(/^8\d{3}$/))
            );
            
            if (transferLikeEvents.length > 0) {
              console.log(`   🔍 Transfer-like events found:`);
              transferLikeEvents.forEach((event, index) => {
                console.log(`   ${index + 1}. ${event.type} - ${event.ext} - ${formatTimestamp(event.last_attempt)}`);
              });
            }
          } catch (e) {
            console.log(`   ⚠️ Could not parse agent history: ${e.message}`);
          }
        }
      }
    } else {
      // Check if it's an outbound call in raw_queue_outbound table
      const outboundQuery = `
        SELECT * FROM raw_queue_outbound 
        WHERE callid = ?
      `;
      
      const outboundResults = await dbService.query(outboundQuery, [callId]);
      
      if (outboundResults.length > 0) {
        const call = outboundResults[0];
        console.log(`📞 CALL TYPE: Outbound Call`);
        console.log(`👤 AGENT: ${call.agent_name} (${call.extension})`);
        console.log(`📅 CALL TIME: ${call.called_time_formatted || formatTimestamp(call.called_time)}`);
        console.log(`📞 CALLER ID: ${call.caller_id_number}`);
        
        // Check for transfer events - outbound calls use agent_history
        const transferResult = detectTransferEvents(call.callid, call.agent_history, 'outbound');
        
        if (transferResult.transfer_event) {
          console.log(`\n✅ BLA TRANSFER DETECTED:`);
          console.log(`   🔄 Transfer Queue: ${transferResult.transfer_queue_extension}`);
          console.log(`   ⏰ Transfer Time: ${formatTimestamp(transferResult.transfer_timestamp)}`);
          console.log(`   📍 Expected Callee ID: ${queueToCalleeExtensionMap[transferResult.transfer_queue_extension] || 'Unknown'}`);
          
          // Search for matching inbound calls
          await searchForInboundMatches(transferResult);
        } else {
          console.log(`\n❌ NO BLA TRANSFER DETECTED`);
          console.log(`   This outbound call does not have transfer events to queue extensions`);
          
          if (call.agent_history) {
            displayAgentHistory(call.agent_history);
          }
        }
      } else {
        // Check if it's an inbound call in raw_queue_inbound table
        const inboundQuery = `
          SELECT * FROM raw_queue_inbound 
          WHERE callid = ?
        `;
        
        const inboundResults = await dbService.query(inboundQuery, [callId]);
      
        if (inboundResults.length > 0) {
          const call = inboundResults[0];
          console.log(`📞 CALL TYPE: Inbound Call`);
          console.log(`👤 AGENT: ${call.agent_name} (${call.extension})`);
          console.log(`📅 CALL TIME: ${call.called_time_formatted || formatTimestamp(call.called_time)}`);
          console.log(`📞 CALLEE ID: ${call.callee_id_number}`);
          
          // Find which queue this callee_id maps to
          const mappedQueue = Object.keys(queueToCalleeExtensionMap).find(
            queue => queueToCalleeExtensionMap[queue] === call.callee_id_number
          );
        
        if (mappedQueue) {
          console.log(`🔄 MAPPED QUEUE: ${mappedQueue} -> ${call.callee_id_number}`);
          
          await searchForCampaignMatches(mappedQueue, call);
        } else {
          console.log(`❌ UNMAPPED CALLEE ID: ${call.callee_id_number} is not mapped to any queue`);
          console.log(`💡 Available mappings:`);
          Object.entries(queueToCalleeExtensionMap).forEach(([queue, calleeId]) => {
            console.log(`   Queue ${queue} -> Callee ID ${calleeId}`);
          });
        }
      } else {
        console.log(`❌ CALL NOT FOUND: ${callId} not found in any table (raw_campaigns, raw_queue_outbound, raw_queue_inbound)`);
      }
      }
    }
    
  } catch (error) {
    console.error(`❌ ERROR: ${error.message}`);
  }
}

async function main() {
  console.log('🔍 BLA Call Checker Tool');
  console.log('This tool analyzes individual calls for BLA transfer linking');
  console.log('Enter a call_id to check if it\'s a BLA transfer and see linking details\n');
  
  const askForCallId = () => {
    rl.question('Enter call_id (or "quit" to exit): ', async (callId) => {
      if (callId.toLowerCase() === 'quit' || callId.toLowerCase() === 'exit') {
        console.log('👋 Goodbye!');
        rl.close();
        process.exit(0);
      }
      
      if (!callId.trim()) {
        console.log('❌ Please enter a valid call_id');
        askForCallId();
        return;
      }
      
      await analyzeCall(callId.trim());
      console.log('\n' + '='.repeat(80));
      askForCallId();
    });
  };
  
  askForCallId();
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n👋 Goodbye!');
  rl.close();
  process.exit(0);
});

main().catch(console.error);
