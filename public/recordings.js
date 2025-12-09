// recordings.js
// JavaScript for Raw Recording Dump functionality

// Global variables
let currentRecordings = [];
let currentPage = 0;
const pageSize = 100;
let isLoading = false;
let hasMoreData = true;
let serialNumberCounter = 0; // Global counter for S.No.

// Infinite scroll state
let scrollState = {
  isScrollLoading: false,
  currentlyDisplayed: 0,
  batchSize: 50 // Number of rows to render at once
};

// DOM elements
const elements = {
  startDate: document.getElementById('startDate'),
  endDate: document.getElementById('endDate'),
  contactNumber: document.getElementById('contactNumber'),
  callId: document.getElementById('callId'),
  fetchBtn: document.getElementById('fetchBtn'),
  exportBtn: document.getElementById('exportBtn'),
  loading: document.getElementById('loading'),
  errorBox: document.getElementById('errorBox'),
  errorMessage: document.getElementById('errorMessage'),
  stats: document.getElementById('stats'),
  statsContent: document.getElementById('statsContent'),
  recordingsTable: document.getElementById('recordingsTable'),
  recordingsTableBody: document.getElementById('recordingsTableBody'),
  noResults: document.getElementById('noResults'),
  filterForm: document.getElementById('filterForm')
};

// Initialize the page
document.addEventListener('DOMContentLoaded', function() {
  console.log('Raw Recording Dump page loaded');
  
  // Set default date range (last 7 days) using datetime-local format
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - 7);
  
  // Format for datetime-local input (YYYY-MM-DDTHH:MM)
  elements.startDate.value = startDate.toISOString().slice(0, 16);
  elements.endDate.value = endDate.toISOString().slice(0, 16);
  
  // Event listeners
  elements.filterForm.addEventListener('submit', function(e) {
    e.preventDefault();
    fetchRecordings();
  });
  elements.exportBtn.addEventListener('click', exportToCSV);
});

// Fetch recordings from the server
async function fetchRecordings(loadMore = false) {
  if (isLoading) return;
  
  try {
    isLoading = true;
    
    if (!loadMore) {
      currentPage = 0;
      currentRecordings = [];
      hasMoreData = true;
      serialNumberCounter = 0; // Reset serial number counter
      elements.recordingsTableBody.innerHTML = '';
      hideAllNotifications();
      
      // Reset scroll state for new search
      scrollState.isScrollLoading = false;
      scrollState.currentlyDisplayed = 0;
      
      // Remove any existing scroll loading indicator
      const existingIndicator = document.getElementById('scroll-loading-indicator');
      if (existingIndicator) {
        existingIndicator.remove();
      }
    }
    
    showLoading();
    elements.fetchBtn.disabled = true;
    
    // Build query parameters
    const params = new URLSearchParams();
    
    if (elements.startDate.value) {
      const startDate = new Date(elements.startDate.value);
      params.append('startDate', Math.floor(startDate.getTime() / 1000));
    }
    
    if (elements.endDate.value) {
      const endDate = new Date(elements.endDate.value);
      params.append('endDate', Math.floor(endDate.getTime() / 1000));
    }
    
    if (elements.contactNumber.value.trim()) {
      params.append('contactNumber', elements.contactNumber.value.trim());
    }
    
    if (elements.callId.value.trim()) {
      params.append('callId', elements.callId.value.trim());
    }
    
    params.append('page', currentPage);
    params.append('pageSize', pageSize);
    
    console.log('Fetching recordings with params:', params.toString());
    
    // Make API request
    const response = await axios.get(`/api/recordings?${params.toString()}`);
    
    if (response.data.success) {
      const recordings = response.data.recordings || [];
      const totalCount = response.data.totalCount || 0;
      const totalProcessed = response.data.totalProcessed || 0;
      const statistics = response.data.statistics || {};
      
      console.log(`Received ${recordings.length} recordings (filtered: ${totalCount}, processed: ${totalProcessed})`);
      console.log('Statistics:', statistics);
      
      if (!loadMore) {
        currentRecordings = recordings;
      } else {
        currentRecordings = [...currentRecordings, ...recordings];
      }
      
      // Check if there's more data
      hasMoreData = recordings.length === pageSize && currentRecordings.length < totalCount;
      
      // Update UI
      if (!loadMore) {
        populateTable(recordings);
      } else {
        appendToTable(recordings, true);
      }
      
      updateStats(currentRecordings.length, totalCount, totalProcessed, statistics);
      
      // Load More button removed - using infinite scroll instead
      
      // Enable export if we have data
      elements.exportBtn.disabled = currentRecordings.length === 0;
      
      // Show no results message if no data
      if (currentRecordings.length === 0) {
        elements.noResults.classList.remove('is-hidden');
      } else {
        elements.noResults.classList.add('is-hidden');
      }
      
    } else {
      throw new Error(response.data.message || 'Failed to fetch recordings');
    }
    
  } catch (error) {
    console.error('Error fetching recordings:', error);
    showError(error.response?.data?.message || error.message || 'Failed to fetch recordings');
  } finally {
    isLoading = false;
    hideLoading();
    elements.fetchBtn.disabled = false;
  }
}

// Load more functionality removed - using infinite scroll instead

// Populate the table with recordings
function populateTable(recordings) {
  elements.recordingsTableBody.innerHTML = '';
  appendToTable(recordings, false);
}

// Append recordings to the table with infinite scrolling
function appendToTable(recordings, isLoadMore = false) {
  const tbody = elements.recordingsTableBody;
  
  if (!isLoadMore) {
    // For new searches, render all records and set up server-side infinite scroll
    renderRecordingRows(tbody, recordings);
    
    // Set up server-side infinite scrolling if there are more records on server
    if (hasMoreData) {
      setupServerInfiniteScroll();
    }
  } else {
    // For load more requests, just append the new records
    renderRecordingRows(tbody, recordings);
  }
  
  // Initialize play buttons
  initializePlayButtons();
}

// Render recording rows
function renderRecordingRows(tbody, recordings) {
  recordings.forEach(recording => {
    const row = document.createElement('tr');
    row.className = 'row-recording';
    
    // Increment serial number counter
    serialNumberCounter++;
    
    // Format the called time
    const calledTime = recording.called_time_formatted || 
                      (recording.called_time ? new Date(recording.called_time * 1000).toLocaleString() : '');
    
    // Handle comma-separated recording IDs
    const recordingIds = recording.recording_id ? recording.recording_id.split(',') : [];
    const recordingButtons = recordingIds
      .filter(id => id && id.trim() !== '')
      .map(recordingId => createRecordingButton(recordingId.trim()))
      .join('<br>');
    
    row.innerHTML = `
      <td>${serialNumberCounter}</td>
      <td>${calledTime}</td>
      <td>${recording.caller_id_number || ''}</td>
      <td>${recording.caller_id_name || ''}</td>
      <td>${recording.callee_id_number || ''}</td>
      <td>${recording.callee_id_name || ''}</td>
      <td>${recording.call_id || ''}</td>
      <td>${recordingButtons}</td>
    `;
    
    tbody.appendChild(row);
  });
}

// Setup server-side infinite scrolling
function setupServerInfiniteScroll() {
  console.log('🚀 Setting up server-side infinite scroll. hasMoreData:', hasMoreData);
  
  // Add loading indicator
  const loadingIndicator = document.createElement('div');
  loadingIndicator.id = 'scroll-loading-indicator';
  loadingIndicator.className = 'has-text-centered p-4';
  loadingIndicator.innerHTML = '<div class=\"loader is-loading\"></div>';
  loadingIndicator.style.display = 'none';
  
  const tableContainer = document.querySelector('.table-container');
  tableContainer.appendChild(loadingIndicator);
  
  // Scroll event handler
  const loadMoreOnScroll = () => {
    if (scrollState.isScrollLoading || !hasMoreData || isLoading) {
      console.log('🚫 Scroll blocked:', { isScrollLoading: scrollState.isScrollLoading, hasMoreData, isLoading });
      return;
    }
    
    const scrollPosition = tableContainer.scrollTop + tableContainer.clientHeight;
    const scrollThreshold = tableContainer.scrollHeight - 200; // Load when within 200px of bottom
    
    console.log('📏 Scroll position:', { scrollPosition, scrollThreshold, scrollHeight: tableContainer.scrollHeight });
    
    if (scrollPosition >= scrollThreshold) {
      scrollState.isScrollLoading = true;
      loadingIndicator.style.display = 'block';
      
      console.log('🔄 Loading more recordings via infinite scroll...');
      
      // Fetch next page from server
      currentPage++;
      fetchRecordings(true).then(() => {
        loadingIndicator.style.display = 'none';
        scrollState.isScrollLoading = false;
        
        // Remove loading indicator if no more data
        if (!hasMoreData) {
          loadingIndicator.remove();
          tableContainer.removeEventListener('scroll', loadMoreOnScroll);
        }
      }).catch((error) => {
        console.error('Error loading more recordings:', error);
        loadingIndicator.style.display = 'none';
        scrollState.isScrollLoading = false;
        currentPage--; // Revert page increment on error
      });
    }
  };
  
  // Attach scroll event listener
  tableContainer.addEventListener('scroll', loadMoreOnScroll);
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
  button.dataset.meta = `${cleanUrl}/meta`;
  
  return button;
}

// Create recording play button (wrapper for backward compatibility)
function createRecordingButton(recordingId) {
  const button = createRecordingLink(recordingId);
  return button ? button.outerHTML : '';
}

// Initialize play button event listeners
function initializePlayButtons() {
  const playButtons = document.querySelectorAll('.play-btn');
  playButtons.forEach(button => {
    // Remove existing listeners to prevent duplicates
    button.replaceWith(button.cloneNode(true));
  });
  
  // Re-select buttons after cloning
  const newPlayButtons = document.querySelectorAll('.play-btn');
  newPlayButtons.forEach(button => {
    button.addEventListener('click', function(e) {
      e.preventDefault();
      const recordingUrl = this.dataset.src || this.dataset.recordingId;
      console.log('Play button clicked, recording URL:', recordingUrl);
      playRecording(recordingUrl);
    });
  });
}

// Play recording in a modal with waveform 
function playRecording(url) {
  console.log('Playing recording:', url);

  if (!url) {
    console.error('Invalid recording URL');
    alert('Error: Invalid recording URL');
    return;
  }

  const fileName = url.split('/').pop().split('?')[0];

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
        <div id="waveform" style="margin-bottom:0.5rem;"></div>
        <div class="compact-player-controls">
          <button class="button is-small" id="rewBtn" title="Rewind 10s"><i class="material-icons">fast_rewind</i></button>
          <button class="button is-primary compact-play-btn" style="border-radius:50%;" id="playBtn" title="Play"><i class="material-icons" id="playIcon">play_arrow</i></button>
          <button class="button is-small" id="fwdBtn" title="Forward 10s"><i class="material-icons">fast_forward</i></button>
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

  // Load Wavesurfer dynamically if needed
  if (!window.WaveSurfer) {
    const script = document.createElement('script');
    script.src = "https://unpkg.com/wavesurfer.js@7/dist/wavesurfer.min.js";
    script.onload = () => initPlayer();
    document.head.appendChild(script);
  } else {
    initPlayer();
  }

  function initPlayer() {
    const wavesurfer = WaveSurfer.create({
      container: '#waveform',
      waveColor: '#90caf9',
      progressColor: '#1976d2',
      cursorColor: '#ef5350',
      barWidth: 2,
      responsive: true,
      backend: 'MediaElement', // HTML5 audio backend
      height: 60,
    });

    // Load file
    wavesurfer.load(url);

    const playBtn = document.getElementById('playBtn');
    const rewBtn = document.getElementById('rewBtn');
    const fwdBtn = document.getElementById('fwdBtn');
    const speedBtn = document.getElementById('speedBtn');
    const timeLbl = document.getElementById('timeLbl');
    const downloadBtn = document.getElementById('downloadBtn');

    // ✅ Fetch duration from backend (/meta) for long recordings
    fetch(`${url}/meta`)
      .then(r => r.json())
      .then(data => {
        if (typeof data.duration === 'number') {
          const dur = data.duration;
          timeLbl.textContent = `0:00 / ${fmt(dur)}`;
        }
      })
      .catch(err => console.error('Meta fetch failed:', err));

    // Play/pause
    playBtn.onclick = () => {
      wavesurfer.playPause();
      document.getElementById('playIcon').textContent =
        wavesurfer.isPlaying() ? 'pause' : 'play_arrow';
    };

    // Rewind/forward
    rewBtn.onclick = () => wavesurfer.skip(-10);
    fwdBtn.onclick = () => wavesurfer.skip(10);

    // Speed control
    let speeds = [1, 1.25, 1.5, 2];
    speedBtn.onclick = () => {
      const cur = wavesurfer.getPlaybackRate();
      const idx = (speeds.indexOf(cur) + 1) % speeds.length;
      const next = speeds[idx];
      wavesurfer.setPlaybackRate(next);
      speedBtn.textContent = next + 'x';
    };

    // Update time label
    wavesurfer.on('audioprocess', () => {
      const cur = wavesurfer.getCurrentTime();
      const dur = wavesurfer.getDuration();
      timeLbl.textContent = `${fmt(cur)} / ${dur ? fmt(dur) : '--:--'}`;
    });
    wavesurfer.on('ready', () => {
      const dur = wavesurfer.getDuration();
      timeLbl.textContent = `0:00 / ${fmt(dur)}`;
    });

    // Download link
    fetch(url)
      .then(r => r.blob())
      .then(blob => {
        const objUrl = URL.createObjectURL(blob);
        downloadBtn.href = objUrl;
        downloadBtn.download = fileName.endsWith('.mp3')
          ? fileName
          : fileName + '.mp3';
      });

    // ✅ Closing modal (X, background, Esc)
    const closeModal = () => {
      wavesurfer.destroy();
      document.body.removeChild(modal);
    };
    modal.querySelector('.modal-background').onclick = closeModal; // click outside
    modal.querySelector('.delete').onclick = closeModal;            // X button
    document.addEventListener('keydown', e => {
      if (e.key === 'Escape') closeModal();
    });
  }

  function fmt(sec) {
    if (!isFinite(sec)) return '--:--';
    const m = Math.floor(sec / 60);
    const s = Math.floor(sec % 60).toString().padStart(2, '0');
    return `${m}:${s}`;
  }
}

// Format time in MM:SS format
function formatTime(seconds) {
  if (isNaN(seconds) || seconds === Infinity) return '--:--';
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  return `${mins}:${secs.toString().padStart(2, '0')}`;
}



// Export recordings to CSV
function exportToCSV() {
  if (currentRecordings.length === 0) {
    alert('No data to export');
    return;
  }
  
  console.log('Exporting recordings to CSV...');
  
  // CSV headers
  const headers = [
    'S.No.',
    'Called Time',
    'Caller ID Number',
    'Caller ID Name',
    'Callee ID Number',
    'Callee ID Name',
    'Call ID',
    'Recording IDs'
  ];
  
  // Build CSV content
  let csvContent = headers.join(',') + '\n';
  
  currentRecordings.forEach((recording, index) => {
    // Recording IDs are already comma-separated, replace commas with semicolons for CSV
    const recordingIds = (recording.recording_id || '').replace(/,/g, '; ');
    
    const row = [
      `"${index + 1}"`, // S.No.
      `"${(recording.called_time_formatted || '').replace(/"/g, '""')}"`,
      `"${(recording.caller_id_number || '').replace(/"/g, '""')}"`,
      `"${(recording.caller_id_name || '').replace(/"/g, '""')}"`,
      `"${(recording.callee_id_number || '').replace(/"/g, '""')}"`,
      `"${(recording.callee_id_name || '').replace(/"/g, '""')}"`,
      `"${(recording.call_id || '').replace(/"/g, '""')}"`,
      `"${recordingIds.replace(/"/g, '""')}"`
    ].join(',');
    
    csvContent += row + '\n';
  });
  
  // Create and download file
  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');
  const url = URL.createObjectURL(blob);
  link.setAttribute('href', url);
  link.setAttribute('download', `raw_recordings_${new Date().toISOString().split('T')[0]}.csv`);
  link.style.visibility = 'hidden';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  
  console.log('CSV export completed');
}

// Update statistics display
function updateStats(currentCount, totalCount, totalProcessed, statistics) {
  let statsText = `Showing ${currentCount} of ${totalCount} recordings`;
  
  if (totalProcessed && totalProcessed !== totalCount) {
    statsText += ` (${totalProcessed} total processed)`;
  }
  
  if (statistics && statistics.uniqueCalls) {
    statsText += ` • ${statistics.uniqueCalls} unique calls`;
  }
  
  if (statistics && statistics.callDirections) {
    const directions = Object.entries(statistics.callDirections)
      .map(([dir, count]) => `${dir}: ${count}`)
      .join(', ');
    if (directions) {
      statsText += ` • Directions: ${directions}`;
    }
  }
  
  elements.statsContent.textContent = statsText;
  elements.stats.classList.remove('is-hidden');
}

// Show loading indicator
function showLoading() {
  elements.loading.classList.remove('is-hidden');
}

// Hide loading indicator
function hideLoading() {
  elements.loading.classList.add('is-hidden');
}

// Show error message
function showError(message) {
  elements.errorMessage.textContent = message;
  elements.errorBox.classList.remove('is-hidden');
}

// Hide all notifications
function hideAllNotifications() {
  elements.loading.classList.add('is-hidden');
  elements.errorBox.classList.add('is-hidden');
  elements.stats.classList.add('is-hidden');
  elements.noResults.classList.add('is-hidden');
}
