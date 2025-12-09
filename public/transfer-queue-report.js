// public/transfer-queue-report.js

// Dubai timezone for consistent date handling
const TIMEZONE = 'Asia/Dubai';

// Global state for results
const state = {
  currentResults: [],
  totalCount: 0,
  lastSearchParams: null,
  timeoutIds: [], // Array to store timeout IDs
  progressiveLoading: {
    active: false,
    queryId: null,
    currentPage: 1,
    totalPages: 0,
    loadedRecords: 0,
    totalRecords: 0,
    isComplete: false
  }
};

// DOM elements
const elements = {
  form: document.getElementById('filterForm'),
  startInput: document.getElementById('start'),
  endInput: document.getElementById('end'),
  fetchBtn: document.getElementById('fetchBtn'),
  csvBtn: document.getElementById('csvBtn'),
  loading: document.getElementById('loading'),
  errorBox: document.getElementById('errorBox'),
  stats: document.getElementById('stats'),
  resultTable: document.getElementById('resultTable')
};

// Initialize date inputs with current day (Dubai timezone)
function initializeDateInputs() {
  const now = luxon.DateTime.now().setZone(TIMEZONE);
  const startOfDay = now.startOf('day');
  const endOfDay = now.endOf('day');
  
  elements.startInput.value = startOfDay.toFormat("yyyy-MM-dd'T'HH:mm");
  elements.endInput.value = endOfDay.toFormat("yyyy-MM-dd'T'HH:mm");
}

// Format duration in seconds to HH:MM:SS
function formatDuration(duration) {
  // Handle null, undefined, empty strings
  if (duration === null || duration === undefined || duration === '') {
    return '00:00:00';
  }
  
  // Handle string format "HH:MM:SS"
  if (typeof duration === 'string') {
    // If it's already in HH:MM:SS format, return as is
    if (duration.match(/^\d{2}:\d{2}:\d{2}$/)) {
      return duration;
    }
    
    // Handle MySQL TIME format
    if (duration.match(/^\d{2}:\d{2}:\d{2}\.\d+$/)) {
      return duration.split('.')[0]; // Remove microseconds
    }
    
    // Try to convert string to number
    if (!isNaN(duration)) {
      duration = Number(duration);
    } else {
      return '00:00:00';
    }
  }
  
  // Handle numeric values (seconds)
  if (typeof duration === 'number') {
    if (isNaN(duration) || duration < 0) {
      return '00:00:00';
    }
    
    const hours = Math.floor(duration / 3600);
    const minutes = Math.floor((duration % 3600) / 60);
    const seconds = Math.floor(duration % 60);
    
    return [
      hours.toString().padStart(2, '0'),
      minutes.toString().padStart(2, '0'),
      seconds.toString().padStart(2, '0')
    ].join(':');
  }
  
  // If all else fails
  return '00:00:00';
}

// Format date for display
function formatDate(dateString, formattedString) {
  // If we have a pre-formatted string from the database, use it
  if (formattedString && formattedString !== '0000-00-00 00:00:00' && formattedString !== 'undefined') return formattedString;
  
  // Handle empty values
  if (!dateString || dateString === null) return '';
  
  // Handle numeric timestamps
  if (typeof dateString === 'number') {
    const date = new Date(dateString * 1000);
    return date.toLocaleString('en-GB', {
      timeZone: TIMEZONE,
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    });
  }
  
  // Handle string timestamps
  if (typeof dateString === 'string') {
    // If it's already formatted, return as is
    if (dateString.includes('/') && dateString.includes(':')) {
      return dateString;
    }
    
    // Try to parse as timestamp
    const timestamp = parseInt(dateString);
    if (!isNaN(timestamp)) {
      const date = new Date(timestamp * 1000);
      return date.toLocaleString('en-GB', {
        timeZone: TIMEZONE,
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
      });
    }
    
    // Try to parse as ISO date
    const date = new Date(dateString);
    if (!isNaN(date.getTime())) {
      return date.toLocaleString('en-GB', {
        timeZone: TIMEZONE,
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
      });
    }
  }
  
  return dateString || '';
}

// Format date and time for display
function formatDateTime(dateString) {
  if (!dateString || dateString === null) return '';
  
  const date = new Date(dateString);
  if (!isNaN(date.getTime())) {
    return date.toLocaleString('en-GB', {
      timeZone: TIMEZONE,
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    });
  }
  
  return dateString || '';
}

// Format transfer extension with color coding
function formatTransferExtension(extension) {
  if (!extension) return '';
  
  // Check if it's a queue extension (8000-8999)
  if (extension.match(/^8\d{3}$/)) {
    return `<span style="color: red; font-weight: bold;">${extension}</span>`;
  } else {
    // Regular agent extension
    return `<span style="color: green; font-weight: bold;">${extension}</span>`;
  }
}

// Format queue extension with highlighting
function formatQueueExtension(extension) {
  if (!extension) return '';
  
  // Queue extensions are always highlighted in red
  return `<span style="color: red; font-weight: bold; background-color: #fff3cd; padding: 2px 4px; border-radius: 3px;">${extension}</span>`;
}

// Table headers specific to transfer queue report
const HEADERS = [
  'Called Time',
  'Record Type',
  'Agent Name',
  'Extension',
  'Queue/Campaign',
  'Contact Number',
  'Contact Name',
  'Callee Number',
  'Answer Time',
  'Hangup Time',
  'Wait Duration',
  'Talk Duration',
  'Hold Duration',
  'Agent Hangup',
  'Agent Disposition',
  'Sub Disp 1',
  'Sub Disp 2',
  'Follow up notes',
  'Status',
  'Campaign Type',
  'Abandoned',
  'Country',
  'Transfer Event',
  'Transfer Extension',
  'Transfer Queue Ext',
  'Transfer Type',
  'Agent History',
  'Queue History',
  'Recording',
  'Call ID',
  'CSAT'
];

// Show/hide elements
function showElement(element) {
  element.classList.remove('is-hidden');
}

function hideElement(element) {
  element.classList.add('is-hidden');
}

// Show error message
function showError(message) {
  elements.errorBox.textContent = message;
  showElement(elements.errorBox);
  hideElement(elements.loading);
}

// Clear error message
function clearError() {
  hideElement(elements.errorBox);
}

// Show loading state
function showLoading() {
  showElement(elements.loading);
  clearError();
}

// Hide loading state
function hideLoading() {
  hideElement(elements.loading);
}

// Update statistics display
function updateStats(totals, queryTime) {
  const statsText = `Transfer Queue Records - Campaign: ${totals.Campaign || 0}, Inbound: ${totals.Inbound || 0}, Outbound: ${totals.Outbound || 0}, Total: ${totals.Total || 0} (Query: ${queryTime}ms)`;
  elements.stats.textContent = statsText;
  showElement(elements.stats);
}

// Create table header
function createTableHeader() {
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  
  HEADERS.forEach(header => {
    const th = document.createElement('th');
    th.textContent = header;
    headerRow.appendChild(th);
  });
  
  thead.appendChild(headerRow);
  return thead;
}

// Create table row for a record
function createTableRow(record) {
  const row = document.createElement('tr');
  row.className = 'transfer-queue-row';
  
  // Process agent history
  let agentHistoryContent = '';
  if (record.agent_history) {
    if (typeof record.agent_history === 'string') {
      try {
        const parsedHistory = JSON.parse(record.agent_history);
        agentHistoryContent = historyToHtml(parsedHistory);
      } catch (e) {
        agentHistoryContent = record.agent_history;
      }
    } else if (Array.isArray(record.agent_history)) {
      agentHistoryContent = historyToHtml(record.agent_history);
    }
  }
  
  // Process queue history
  let queueHistoryContent = '';
  if (record.queue_history) {
    if (typeof record.queue_history === 'string') {
      try {
        const parsedHistory = JSON.parse(record.queue_history);
        queueHistoryContent = queueHistoryToHtml(parsedHistory);
      } catch (e) {
        queueHistoryContent = record.queue_history;
      }
    } else if (Array.isArray(record.queue_history)) {
      queueHistoryContent = queueHistoryToHtml(record.queue_history);
    }
  }
  
  // Format data for each column
  const rowData = [
    { value: formatDateTime(record.called_time_formatted), isHTML: false },
    { value: record.record_type || '', isHTML: false },
    { value: record.agent_name || '', isHTML: false },
    { value: record.extension || '', isHTML: false },
    { value: record.queue_campaign_name || '', isHTML: false },
    { value: record.caller_id_number || '', isHTML: false },
    { value: record.caller_id_name || '', isHTML: false },
    { value: record.callee_id_number || '', isHTML: false },
    { value: formatDateTime(record.answered_time), isHTML: false },
    { value: formatDateTime(record.hangup_time), isHTML: false },
    { value: formatDuration(record.wait_duration), isHTML: false },
    { value: formatDuration(record.talk_duration), isHTML: false },
    { value: formatDuration(record.hold_duration), isHTML: false },
    { value: record.agent_hangup || '', isHTML: false },
    { value: record.agent_disposition || '', isHTML: false },
    { value: record.sub_disp_1 || '', isHTML: false },
    { value: record.sub_disp_2 || '', isHTML: false },
    { value: record.follow_up_notes || '', isHTML: false },
    { value: record.status || '', isHTML: false },
    { value: record.campaign_type || '', isHTML: false },
    { value: record.abandoned || '', isHTML: false },
    { value: record.country || '', isHTML: false },
    { value: record.transfer_event || '', isHTML: false },
    { value: formatTransferExtension(record.transfer_extension), isHTML: true },
    { value: formatQueueExtension(record.transfer_queue_extension), isHTML: true },
    { value: record.transfer_type || '', isHTML: false },
    { value: agentHistoryContent, isHTML: true },
    { value: queueHistoryContent, isHTML: true },
    { value: record.recording ? createRecordingLink(record.recording) : '', isHTML: false, isElement: true },
    { value: record.call_id || '', isHTML: false },
    { value: record.csat || '', isHTML: false }
  ];
  
  rowData.forEach((column, index) => {
    const cell = document.createElement('td');
    
    if (column.isHTML) {
      cell.innerHTML = column.value;
      // Initialize eye buttons for history columns after DOM insertion
      if ((index === 26 || index === 27) && column.value) { // Agent History or Queue History columns
        setTimeout(() => {
          initializeEyeButtons(cell);
        }, 0);
      }
    } else if (column.isElement && column.value instanceof Node) {
      // For DOM elements like the recording button
      cell.appendChild(column.value);
    } else if (typeof column.value === 'string') {
      cell.innerHTML = column.value;
    } else {
      cell.textContent = column.value || '';
    }
    
    row.appendChild(cell);
  });
  
  return row;
}

// Create recording link/button
function createRecordingLink(recordingUrl) {
  if (!recordingUrl) return '';
  
  const button = document.createElement('button');
  button.className = 'button is-small is-info is-rounded play-btn';
  button.innerHTML = '<span class="icon is-small"><i class="material-icons">play_arrow</i></span>';
  button.title = 'Play recording';
  
  // Ensure the URL is properly formatted
  let cleanUrl = recordingUrl;
  // If it's just an ID, assume it's a relative path to the API endpoint
  if (!recordingUrl.includes('/') && !recordingUrl.includes('http')) {
    cleanUrl = `/api/recordings/${recordingUrl}?account=default`;
  }
  
  button.dataset.src = cleanUrl;
  
  return button;
}

// Play recording in a modal with waveform 
function playRecording(url) {
  console.log('Playing recording:', url);

  if (!url) {
    console.error('Invalid recording URL');
    alert('Error: Invalid recording URL');
    return;
  }

  const fileName = url.split('/').pop() || 'recording';

  // Create modal
  const modal = document.createElement('div');
  modal.className = 'modal is-active recording-modal';
  modal.innerHTML = `
    <div class="modal-background"></div>
    <div class="modal-card">
      <header class="modal-card-head font-size-xs">
        <p class="modal-card-title font-size-0.5rem">Recording: ${fileName}</p>
        <button class="delete" aria-label="close"></button>
      </header>
      <section class="modal-card-body">
        <div style="position:relative;">
          <audio id="audioPlayer" controls style="width:100%; margin-bottom:10px;">
            <source src="${url}" type="audio/mpeg">
            Your browser does not support the audio element.
          </audio>
          <span id="timeLbl" style="font-size:0.8rem;">0:00 / --:--</span>
          <button class="button is-small is-light" id="speedBtn" title="Switch speed">1x</button>
        </div>
        <div style="position:absolute; bottom:10px; right:5px;" title="Download Recording">
          <a id="downloadBtn"><i class="material-icons">file_download</i></a>
        </div>
      </section>
    </div>
  `;

  document.body.appendChild(modal);

  // Get elements
  const audioPlayer = document.getElementById('audioPlayer');
  const closeBtn = modal.querySelector('.delete');
  const modalBg = modal.querySelector('.modal-background');
  const speedBtn = document.getElementById('speedBtn');
  const timeLbl = document.getElementById('timeLbl');
  const downloadBtn = document.getElementById('downloadBtn');

  // ✅ Fetch duration from backend (/meta) for long recordings
  fetch(`${url}/meta`)
    .then(r => r.json())
    .then(data => {
      if (data.duration) {
        const totalMinutes = Math.floor(data.duration / 60);
        const totalSeconds = Math.floor(data.duration % 60);
        timeLbl.textContent = `0:00 / ${totalMinutes}:${totalSeconds.toString().padStart(2, '0')}`;
      }
    })
    .catch(() => {
      // Fallback: wait for loadedmetadata
      audioPlayer.addEventListener('loadedmetadata', () => {
        const totalMinutes = Math.floor(audioPlayer.duration / 60);
        const totalSeconds = Math.floor(audioPlayer.duration % 60);
        timeLbl.textContent = `0:00 / ${totalMinutes}:${totalSeconds.toString().padStart(2, '0')}`;
      });
    });

  // Time update
  audioPlayer.addEventListener('timeupdate', () => {
    const currentMinutes = Math.floor(audioPlayer.currentTime / 60);
    const currentSeconds = Math.floor(audioPlayer.currentTime % 60);
    const totalMinutes = Math.floor(audioPlayer.duration / 60);
    const totalSeconds = Math.floor(audioPlayer.duration % 60);
    
    timeLbl.textContent = `${currentMinutes}:${currentSeconds.toString().padStart(2, '0')} / ${totalMinutes}:${totalSeconds.toString().padStart(2, '0')}`;
  });

  // Speed control
  let currentSpeed = 1;
  const speeds = [1, 1.25, 1.5, 2];
  speedBtn.addEventListener('click', () => {
    const currentIndex = speeds.indexOf(currentSpeed);
    const nextIndex = (currentIndex + 1) % speeds.length;
    currentSpeed = speeds[nextIndex];
    audioPlayer.playbackRate = currentSpeed;
    speedBtn.textContent = `${currentSpeed}x`;
  });

  // Download button
  downloadBtn.href = url;
  downloadBtn.download = fileName;

  // Close modal
  function closeModal() {
    audioPlayer.pause();
    document.body.removeChild(modal);
  }

  closeBtn.addEventListener('click', closeModal);
  modalBg.addEventListener('click', closeModal);

  // Auto-play
  audioPlayer.play().catch(e => {
    console.log('Auto-play prevented:', e);
  });
}

// Convert Agent history array into an HTML table for display
function historyToHtml(history) {
  if (!Array.isArray(history) || !history.length) return '';

  // Ensure ascending order by last_attempt (oldest first)
  const sortedHistory = [...history].sort((a, b) => {
    const timeA = a.last_attempt || 0;
    const timeB = b.last_attempt || 0;
    return timeA - timeB;
  });

  const thead = '<thead><tr><th>Date</th><th>Name</th><th>Ext</th><th>Type</th><th>Event</th><th>Connected</th><th>Queue</th></tr></thead>';
  const rows = sortedHistory.map(h => {
    const date = h.last_attempt ? new Date(h.last_attempt * 1000).toLocaleString('en-GB', {
      timeZone: TIMEZONE,
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    }) : '';
    
    return `<tr>
      <td>${date}</td>
      <td>${h.first_name || ''} ${h.last_name || ''}</td>
      <td>${h.ext || ''}</td>
      <td>${h.type || ''}</td>
      <td>${h.event || ''}</td>
      <td>${h.connected || ''}</td>
      <td>${h.queue || ''}</td>
    </tr>`;
  }).join('');

  const eyeButton = '<button class="button is-small is-rounded eye-btn" title="View Details"><span class="icon is-small"><i class="material-icons">visibility</i></span></button>';
  return `${eyeButton}<div class="history-content is-hidden"><table class="table is-narrow is-striped">${thead}<tbody>${rows}</tbody></table></div>`;
}

// Convert Queue history array into an HTML table for display
function queueHistoryToHtml(history) {
  if (!Array.isArray(history) || !history.length) return '';
  const thead = '<thead><tr><th>Date</th><th>Queue Name</th></tr></thead>';
  const rows = history.map(h => {
    const date = h.last_attempt ? new Date(h.last_attempt * 1000).toLocaleString('en-GB', {
      timeZone: TIMEZONE,
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    }) : '';
    
    return `<tr><td>${date}</td><td>${h.queue || ''}</td></tr>`;
  }).join('');

  const eyeButton = '<button class="button is-small is-rounded eye-btn" title="View Details"><span class="icon is-small"><i class="material-icons">visibility</i></span></button>';
  return `${eyeButton}<div class="history-content is-hidden"><table class="table is-narrow is-striped">${thead}<tbody>${rows}</tbody></table></div>`;
}

// Initialize eye buttons for history viewing
function initializeEyeButtons(container) {
  const eyeButtons = container.querySelectorAll('.eye-btn');
  eyeButtons.forEach(btn => {
    btn.addEventListener('click', function(e) {
      e.preventDefault();
      const historyContent = this.nextElementSibling;
      if (historyContent && historyContent.classList.contains('history-content')) {
        historyContent.classList.toggle('is-hidden');
        const icon = this.querySelector('i');
        if (historyContent.classList.contains('is-hidden')) {
          icon.textContent = 'visibility';
          this.title = 'View Details';
        } else {
          icon.textContent = 'visibility_off';
          this.title = 'Hide Details';
        }
      }
    });
  });
}

// Render table with results
function renderTable(results) {
  // Clear existing table
  elements.resultTable.innerHTML = '';
  
  if (!results || results.length === 0) {
    elements.resultTable.innerHTML = '<tr><td colspan="20">No transfer queue records found</td></tr>';
    return;
  }
  
  // Create header
  const thead = createTableHeader();
  elements.resultTable.appendChild(thead);
  
  // Create body
  const tbody = document.createElement('tbody');
  
  results.forEach(record => {
    const row = createTableRow(record);
    tbody.appendChild(row);
  });
  
  elements.resultTable.appendChild(tbody);
}

// Fetch transfer queue reports
async function fetchTransferQueueReports() {
  const formData = new FormData(elements.form);
  const params = new URLSearchParams();
  
  // Add form data to params
  for (const [key, value] of formData.entries()) {
    if (value.trim() !== '') {
      params.append(key, value.trim());
    }
  }
  
  // Add fetchAll parameter
  params.append('fetchAll', 'true');
  
  // Debug: Log form data
  console.log('Form data entries:');
  for (const [key, value] of formData.entries()) {
    console.log(`  ${key}: "${value}"`);
  }
  console.log('URL params:', params.toString());
  
  try {
    showLoading();
    clearError();
    
    console.log('Fetching transfer queue reports with params:', params.toString());
    
    const response = await axios.get(`/api/reports/transfer-queue?${params.toString()}`);
    
    if (response.data.success) {
      state.currentResults = response.data.data || [];
      state.totalCount = response.data.totals?.Total || 0;
      
      renderTable(state.currentResults);
      updateStats(response.data.totals, response.data.query_time_ms);
      
      // Enable CSV download if we have results
      elements.csvBtn.disabled = state.currentResults.length === 0;
      
      console.log(`Loaded ${state.currentResults.length} transfer queue records`);
    } else {
      showError(response.data.message || 'Failed to fetch transfer queue reports');
    }
  } catch (error) {
    console.error('Error fetching transfer queue reports:', error);
    
    if (error.response) {
      const errorMsg = error.response.data?.error || error.response.data?.message || `Server error: ${error.response.status}`;
      showError(errorMsg);
    } else if (error.request) {
      showError('Network error: Unable to connect to server');
    } else {
      showError(`Error: ${error.message}`);
    }
  } finally {
    hideLoading();
  }
}

// Generate CSV content
function generateCSV() {
  if (!state.currentResults || state.currentResults.length === 0) {
    alert('No data to export');
    return;
  }
  
  const csvHeaders = HEADERS.join(',');
  const csvRows = state.currentResults.map(record => {
    return [
      formatDateTime(record.called_time_formatted),
      record.record_type || '',
      record.agent_name || '',
      record.extension || '',
      record.queue_campaign_name || '',
      record.caller_id_number || '',
      record.caller_id_name || '',
      record.callee_id_number || '',
      formatDateTime(record.answered_time),
      formatDateTime(record.hangup_time),
      formatDuration(record.wait_duration),
      formatDuration(record.talk_duration),
      formatDuration(record.hold_duration),
      record.agent_hangup || '',
      record.agent_disposition || '',
      record.sub_disp_1 || '',
      record.sub_disp_2 || '',
      record.follow_up_notes || '',
      record.status || '',
      record.campaign_type || '',
      record.abandoned || '',
      record.country || '',
      record.transfer_event || '',
      formatTransferExtension(record.transfer_extension),
      formatQueueExtension(record.transfer_queue_extension),
      record.transfer_type || '',
      record.agent_history || '',
      record.queue_history || '',
      record.recording || '',
      record.call_id || '',
      record.csat || ''
    ].map(field => `"${String(field).replace(/"/g, '""')}"`).join(',');
  });
  
  const csvContent = [csvHeaders, ...csvRows].join('\n');
  
  // Create and download file
  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');
  const url = URL.createObjectURL(blob);
  link.setAttribute('href', url);
  link.setAttribute('download', `transfer_queue_report_${new Date().toISOString().split('T')[0]}.csv`);
  link.style.visibility = 'hidden';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

// Event listeners
document.addEventListener('DOMContentLoaded', function() {
  // Initialize date inputs
  initializeDateInputs();
  
  // Form submission
  elements.form.addEventListener('submit', function(e) {
    e.preventDefault();
    fetchTransferQueueReports();
  });
  
  // CSV download
  elements.csvBtn.addEventListener('click', function(e) {
    e.preventDefault();
    generateCSV();
  });
  
  // Recording play buttons (delegated event listener)
  document.addEventListener('click', function(e) {
    if (e.target.closest('.play-btn')) {
      e.preventDefault();
      const btn = e.target.closest('.play-btn');
      console.log('Play button clicked, src:', btn.dataset.src);
      playRecording(btn.dataset.src);
    }
  });
  
  console.log('Transfer Queue Report page initialized');
});
