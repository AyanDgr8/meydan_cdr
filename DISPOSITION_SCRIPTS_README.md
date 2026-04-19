# Disposition Fill Scripts

## Overview
Two scripts to automatically fill empty disposition fields in the `final_report` table by fetching data from remote MySQL database.

## Scripts

### 1. `fill_empty_dispositions_simple.js` (Inbound Calls)
Fills empty dispositions for **Inbound** calls.

**Usage:**
```bash
# Last 24 hours (default)
node fill_empty_dispositions_simple.js

# Last 30 hours
node fill_empty_dispositions_simple.js 30

# Last 48 hours
node fill_empty_dispositions_simple.js 48
```

### 2. `fill_empty_dispositions_outbound.js` (Outbound Calls)
Fills empty dispositions for **Outbound** calls.

**Usage:**
```bash
# Last 24 hours (default)
node fill_empty_dispositions_outbound.js

# Last 30 hours
node fill_empty_dispositions_outbound.js 30

# Last 48 hours
node fill_empty_dispositions_outbound.js 48
```

## How It Works

### Data Sources (Priority Order)
Both scripts use a **two-tier fallback mechanism**:

1. **Primary Source: `forms_new` table**
   - Matches records by phone number and timestamp (±2 hour window)
   - For Inbound: matches `caller_id_number`
   - For Outbound: matches `callee_id_number`

2. **Fallback Source: `webhook_logs` table**
   - If no match found in `forms_new`, queries `webhook_logs` using `call_id`
   - Extracts disposition data from JSON payload
   - Payload structure:
     ```json
     {
       "disposition": "Request",
       "subdisposition": {
         "name": "Company cancellation",
         "subdisposition": {
           "name": "Suspended company_B2B",
           "subdisposition": {
             "name": "Resolved"
           }
         }
       },
       "follow_up_notes": "submitting before submission"
     }
     ```

### Features
- ✅ SSH connection to remote database with persistent ControlMaster
- ✅ ±2 hour time window for matching (accounts for delayed form submissions)
- ✅ Converts "General Enquiry" to "GENERAL ENQUIRY"
- ✅ Skips NULL values in output
- ✅ Shows summary of updated records with data source
- ✅ Comprehensive logging with timestamps
- ✅ Error handling and retry logic

### Fields Updated
- `agent_disposition`
- `sub_disp_1`
- `sub_disp_2`
- `sub_disp_3`
- `follow_up_notes`

### Filters
Only processes records where:
- `record_type` = 'Inbound' or 'Outbound'
- `agent_disposition` IS NULL or empty
- `agent_name` IS NOT NULL and not empty
- Within specified lookback period

## Output Example

```
Processing record ID 640325 (0567719069)...
  Raw output: 0567719069     General Enquiry Others  Others  Resolved...
  ✓ Match found for 0567719069: time diff 1234s
  ✅ Updated record ID 640325 (source: forms_new):
     - agent_disposition: GENERAL ENQUIRY
     - sub_disp_1: Others
     - sub_disp_2: Others
     - sub_disp_3: Resolved
     - follow_up_notes: Resolved

Processing record ID 640293 (+97147777333)...
  No output from remote query for +97147777333
  Trying webhook_logs for call_id: abc123xyz...
  ✓ Found webhook_logs entry for call_id: abc123xyz
  ✅ Updated record ID 640293 (source: webhook_logs):
     - agent_disposition: Request
     - sub_disp_1: Company cancellation
     - sub_disp_2: Suspended company_B2B

========================================
Process Completed
========================================
Total records processed: 27
Successfully updated: 26
No match found: 1
Errors: 0
Duration: 25.61s
========================================

📋 Updated Records Summary:
========================================
1. Record ID: 640325 [forms_new]
   Phone: 0567719069
   Disposition: GENERAL ENQUIRY > Others > Others
2. Record ID: 640293 [webhook_logs]
   Phone: +97147777333
   Disposition: Request > Company cancellation > Suspended company_B2B
========================================
```

## Remote Database Details
- **Server:** 94.206.56.70:11446
- **Database:** meydanform
- **Tables:** 
  - `forms_new` (primary source)
  - `webhook_logs` (fallback source)
- **Connection:** SSH tunnel via PEM key

## Notes
- SSH passphrase required on first run (avesun123)
- Scripts use ControlMaster for connection reuse
- Time window increased to ±2 hours to capture delayed submissions
- Both tables are checked in sequence for maximum coverage
