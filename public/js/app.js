/**
 * AirShare Main Application Controller & UI Logic
 */

document.addEventListener('DOMContentLoaded', async () => {
  // Initialize Lucide icons
  lucide.createIcons();

  // Socket.io connection (Optional on Vercel)
  const socket = typeof io !== 'undefined' ? io({ reconnection: true, autoConnect: true }) : null;

  // App State
  let currentRoomId = '';
  let selfSocketId = '';
  let networkInfo = { localIp: 'localhost', port: 3000, lanUrl: '' };
  let peers = []; // list of connected peers in current room
  let fileQueue = []; // files selected for upload
  let historyItems = []; // completed transfers

  // Fallback incoming transfers storage over Socket.io relay
  const relayIncomingTransfers = new Map();
  const activeRelayOutgoings = new Map();

  // WebRTC Manager Instance
  const webrtcManager = new WebRTCManager(socket, {
    onProgress: (transferId, stats) => updateTransferProgressUI(transferId, stats),
    onComplete: (transferId) => completeTransferUI(transferId),
    onError: (transferId, errorMsg) => errorTransferUI(transferId, errorMsg),
    onFileReceived: (fileData) => handleReceivedFile(fileData)
  });

  // System & Browser Info Detection
  const deviceInfo = detectDeviceDetails();

  // DOM Element Selectors
  const networkBadge = document.getElementById('networkBadge');
  const networkIpText = document.getElementById('networkIpText');
  const currentRoomCode = document.getElementById('currentRoomCode');
  const connectionStatusDot = document.getElementById('connectionStatusDot');
  const roomCodeInput = document.getElementById('roomCodeInput');
  const btnJoinRoom = document.getElementById('btnJoinRoom');
  const btnCreateRoom = document.getElementById('btnCreateRoom');
  const btnCopyLink = document.getElementById('btnCopyLink');
  const btnShowQr = document.getElementById('btnShowQr');
  
  const peerCount = document.getElementById('peerCount');
  const devicesGrid = document.getElementById('devicesGrid');
  const selfDeviceName = document.getElementById('selfDeviceName');
  const selfDeviceMeta = document.getElementById('selfDeviceMeta');

  const dropzone = document.getElementById('dropzone');
  const fileInput = document.getElementById('fileInput');
  const fileQueueContainer = document.getElementById('fileQueueContainer');
  const queueCount = document.getElementById('queueCount');
  const queueList = document.getElementById('queueList');
  const btnClearQueue = document.getElementById('btnClearQueue');
  const targetPeerSelect = document.getElementById('targetPeerSelect');
  const btnSendFiles = document.getElementById('btnSendFiles');

  const transfersCard = document.getElementById('transfersCard');
  const transfersList = document.getElementById('transfersList');
  const emptyTransfersState = document.getElementById('emptyTransfersState');
  const activeTransfersCount = document.getElementById('activeTransfersCount');

  const historyList = document.getElementById('historyList');
  const emptyHistoryState = document.getElementById('emptyHistoryState');
  const btnClearHistory = document.getElementById('btnClearHistory');

  const qrModal = document.getElementById('qrModal');
  const btnCloseQr = document.getElementById('btnCloseQr');
  const qrCodeImg = document.getElementById('qrCodeImg');
  const qrUrlInput = document.getElementById('qrUrlInput');
  const btnCopyQrUrl = document.getElementById('btnCopyQrUrl');
  const qrRoomCodeDisplay = document.getElementById('qrRoomCodeDisplay');

  const previewModal = document.getElementById('previewModal');
  const btnClosePreview = document.getElementById('btnClosePreview');
  const previewTitle = document.getElementById('previewTitle');
  const previewBody = document.getElementById('previewBody');

  // Update Self Device Meta UI
  selfDeviceName.textContent = `${deviceInfo.deviceName} (You)`;
  selfDeviceMeta.textContent = `${deviceInfo.osName} • ${deviceInfo.browserName}`;

  // Fetch Network Info & URL Params
  await fetchNetworkInfo();
  checkUrlParamsAndJoinRoom();

  // Socket.io Handlers
  socket.on('connect', () => {
    selfSocketId = socket.id;
    connectionStatusDot.classList.add('online');
    console.log('[Socket] Connected with ID:', socket.id);
  });

  socket.on('disconnect', () => {
    connectionStatusDot.classList.remove('online');
    console.log('[Socket] Disconnected');
  });

  socket.on('joined-room-success', ({ roomId, selfId, peers: roomPeers }) => {
    currentRoomId = roomId;
    currentRoomCode.textContent = roomId;
    qrRoomCodeDisplay.textContent = roomId;
    updatePeersUI(roomPeers);

    // Update URL hash/query without reload
    const newUrl = new URL(window.location.href);
    newUrl.searchParams.set('room', roomId);
    window.history.pushState({}, '', newUrl);
  });

  socket.on('room-peers', (roomPeers) => {
    updatePeersUI(roomPeers);
  });

  // Socket.io Relayed File Transfer Events (Fallback Engine)
  socket.on('relay-file-init', ({ senderSocketId, fileMeta }) => {
    relayIncomingTransfers.set(fileMeta.transferId, {
      senderSocketId,
      metadata: fileMeta,
      chunks: [],
      receivedBytes: 0,
      totalBytes: fileMeta.size,
      startTime: Date.now()
    });

    createTransferCardUI({
      transferId: fileMeta.transferId,
      fileName: fileMeta.name,
      fileSize: fileMeta.size,
      type: 'receiving',
      channel: 'Relayed Stream'
    });

    socket.emit('relay-file-response', {
      targetSocketId: senderSocketId,
      fileId: fileMeta.transferId,
      accepted: true
    });
  });

  socket.on('relay-file-chunk', ({ senderSocketId, fileId, chunkIndex, totalChunks, chunkData }) => {
    const transfer = relayIncomingTransfers.get(fileId);
    if (!transfer) return;

    transfer.chunks.push(chunkData);
    transfer.receivedBytes += chunkData.byteLength || chunkData.length || 0;

    const progress = Math.min(100, Math.round((transfer.receivedBytes / transfer.totalBytes) * 100));
    const elapsedSec = (Date.now() - transfer.startTime) / 1000;
    const speedBps = elapsedSec > 0 ? transfer.receivedBytes / elapsedSec : 0;
    const remainingBytes = transfer.totalBytes - transfer.receivedBytes;
    const etaSec = speedBps > 0 ? remainingBytes / speedBps : 0;

    updateTransferProgressUI(fileId, {
      progress,
      receivedBytes: transfer.receivedBytes,
      totalBytes: transfer.totalBytes,
      speedBps,
      etaSec,
      channel: 'Relayed Stream'
    });
  });

  socket.on('relay-file-complete', ({ senderSocketId, fileId }) => {
    const transfer = relayIncomingTransfers.get(fileId);
    if (!transfer) return;

    const fileBlob = new Blob(transfer.chunks, { type: transfer.metadata.mimeType || 'application/octet-stream' });
    relayIncomingTransfers.delete(fileId);

    completeTransferUI(fileId);

    handleReceivedFile({
      transferId: fileId,
      name: transfer.metadata.name,
      size: transfer.metadata.size,
      mimeType: transfer.metadata.mimeType,
      blob: fileBlob,
      senderSocketId
    });
  });

  socket.on('relay-file-cancel', ({ senderSocketId, fileId }) => {
    if (relayIncomingTransfers.has(fileId)) {
      relayIncomingTransfers.delete(fileId);
      errorTransferUI(fileId, 'Cancelled by sender');
    }
  });

  // UI Event Listeners
  btnJoinRoom.addEventListener('click', () => {
    const code = roomCodeInput.value.trim();
    if (code.length === 6) {
      joinRoom(code);
    } else {
      alert('Please enter a valid 6-digit room code.');
    }
  });

  roomCodeInput.addEventListener('keyup', (e) => {
    if (e.key === 'Enter') {
      btnJoinRoom.click();
    }
  });

  btnCreateRoom.addEventListener('click', async () => {
    const res = await fetch('/api/room/new');
    const data = await res.json();
    joinRoom(data.roomId);
  });

  btnCopyLink.addEventListener('click', () => {
    const shareUrl = `${window.location.origin}?room=${currentRoomId}`;
    navigator.clipboard.writeText(shareUrl);
    showToast('Direct invite link copied to clipboard!');
  });

  btnShowQr.addEventListener('click', () => {
    openQrModal();
  });

  btnCloseQr.addEventListener('click', () => {
    qrModal.classList.remove('active');
  });

  btnCopyQrUrl.addEventListener('click', () => {
    navigator.clipboard.writeText(qrUrlInput.value);
    showToast('QR Code link copied!');
  });

  btnClosePreview.addEventListener('click', () => {
    previewModal.classList.remove('active');
    previewBody.innerHTML = '';
  });

  // File Dropzone Listeners
  dropzone.addEventListener('click', () => fileInput.click());

  fileInput.addEventListener('change', (e) => {
    addFilesToQueue(Array.from(e.target.files));
    fileInput.value = '';
  });

  ['dragenter', 'dragover'].forEach(eventName => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.add('dragover');
    }, false);
  });

  ['dragleave', 'drop'].forEach(eventName => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.remove('dragover');
    }, false);
  });

  dropzone.addEventListener('drop', (e) => {
    const dt = e.dataTransfer;
    const files = Array.from(dt.files);
    addFilesToQueue(files);
  });

  btnClearQueue.addEventListener('click', () => {
    fileQueue = [];
    renderQueueUI();
  });

  btnSendFiles.addEventListener('click', () => {
    if (fileQueue.length === 0) return;
    const targetPeerId = targetPeerSelect.value;
    const filesToSend = [...fileQueue];
    fileQueue = [];
    renderQueueUI();

    filesToSend.forEach(file => {
      if (targetPeerId === 'all') {
        // Send to all peers in room except self
        const otherPeers = peers.filter(p => p.socketId !== selfSocketId);
        if (otherPeers.length === 0) {
          alert('No other devices connected in this room! Scan QR code on your phone or open another device tab to connect.');
          return;
        }
        otherPeers.forEach(peer => initiateFileTransfer(file, peer.socketId));
      } else {
        initiateFileTransfer(file, targetPeerId);
      }
    });
  });

  btnClearHistory.addEventListener('click', () => {
    historyItems = [];
    renderHistoryUI();
  });

  // Core Transfer Logic
  async function initiateFileTransfer(file, targetSocketId) {
    const transferId = generateUuid();
    
    createTransferCardUI({
      transferId,
      fileName: file.name,
      fileSize: file.size,
      type: 'sending',
      channel: 'WebRTC P2P (Connecting...)'
    });

    // Attempt 1: Fast WebRTC Direct P2P Channel
    const p2pSuccess = await webrtcManager.sendFileP2P(targetSocketId, file, transferId);

    if (!p2pSuccess) {
      console.log(`[Transfer] P2P fallback triggered for file ${file.name}. Using Socket.io relay stream.`);
      updateTransferChannelUI(transferId, 'Relayed Stream');
      await sendFileViaSocketRelay(file, targetSocketId, transferId);
    }
  }

  // Socket.io Chunked Relay Implementation (Fallback stream)
  async function sendFileViaSocketRelay(file, targetSocketId, transferId) {
    activeRelayOutgoings.set(transferId, { cancelled: false });

    // 1. Send file metadata
    socket.emit('relay-file-init', {
      targetSocketId,
      fileMeta: {
        transferId,
        name: file.name,
        size: file.size,
        mimeType: file.type || 'application/octet-stream'
      }
    });

    // 2. Stream 64KB chunks over Socket.io
    const CHUNK_SIZE = 64 * 1024;
    let offset = 0;
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    let chunkIndex = 0;
    const startTime = Date.now();

    while (offset < file.size) {
      const state = activeRelayOutgoings.get(transferId);
      if (!state || state.cancelled) {
        socket.emit('relay-file-cancel', { targetSocketId, fileId: transferId });
        return;
      }

      const slice = file.slice(offset, offset + CHUNK_SIZE);
      const chunkBuffer = await slice.arrayBuffer();

      socket.emit('relay-file-chunk', {
        targetSocketId,
        fileId: transferId,
        chunkIndex,
        totalChunks,
        chunkData: chunkBuffer
      });

      offset += chunkBuffer.byteLength;
      chunkIndex++;

      const progress = Math.min(100, Math.round((offset / file.size) * 100));
      const elapsedSec = (Date.now() - startTime) / 1000;
      const speedBps = elapsedSec > 0 ? offset / elapsedSec : 0;
      const remainingBytes = file.size - offset;
      const etaSec = speedBps > 0 ? remainingBytes / speedBps : 0;

      updateTransferProgressUI(transferId, {
        progress,
        sentBytes: offset,
        totalBytes: file.size,
        speedBps,
        etaSec,
        channel: 'Relayed Stream'
      });

      // Small throttle to avoid flooding socket loop
      await new Promise(r => setTimeout(r, 8));
    }

    socket.emit('relay-file-complete', { targetSocketId, fileId: transferId });
    completeTransferUI(transferId);
    activeRelayOutgoings.delete(transferId);
  }

  // Handle Received File (Trigger Download + History item)
  function handleReceivedFile({ transferId, name, size, mimeType, blob }) {
    const url = URL.createObjectURL(blob);
    
    // Automatically add to history list
    const item = {
      id: transferId,
      name,
      size,
      mimeType,
      url,
      blob,
      timestamp: new Date().toLocaleTimeString()
    };
    historyItems.unshift(item);
    renderHistoryUI();

    showToast(`Received file: ${name}`);
  }

  // UI Helpers & Renderers
  function joinRoom(roomId) {
    currentRoomId = roomId;
    currentRoomCode.textContent = roomId;
    qrRoomCodeDisplay.textContent = roomId;

    if (socket && socket.connected) {
      socket.emit('join-room', {
        roomId,
        deviceName: deviceInfo.deviceName,
        deviceType: deviceInfo.deviceType,
        osName: deviceInfo.osName,
        browserName: deviceInfo.browserName
      });
    }

    // Initialize PeerJS for Vercel / Cloud WebRTC P2P
    webrtcManager.initPeerJs(roomId, deviceInfo, (peerId) => {
      console.log('[App] PeerJS connected:', peerId);
    });

    const newUrl = new URL(window.location.href);
    newUrl.searchParams.set('room', roomId);
    window.history.pushState({}, '', newUrl);
  }

  function updatePeersUI(roomPeers) {
    peers = roomPeers;
    const otherPeers = peers.filter(p => p.socketId !== selfSocketId);
    peerCount.textContent = peers.length;

    // Render device grid
    devicesGrid.innerHTML = '';

    // Always render Self card first
    const selfDiv = document.createElement('div');
    selfDiv.className = 'device-item self';
    selfDiv.innerHTML = `
      <div class="device-icon"><i data-lucide="${getDeviceIcon(deviceInfo.deviceType)}"></i></div>
      <div class="device-details">
        <div class="device-name">${deviceInfo.deviceName} (You)</div>
        <div class="device-meta">${deviceInfo.osName} • ${deviceInfo.browserName}</div>
      </div>
      <span class="badge self-badge">YOU</span>
    `;
    devicesGrid.appendChild(selfDiv);

    // Render other connected peers
    otherPeers.forEach(peer => {
      const peerDiv = document.createElement('div');
      peerDiv.className = 'device-item';
      peerDiv.innerHTML = `
        <div class="device-icon"><i data-lucide="${getDeviceIcon(peer.deviceType)}"></i></div>
        <div class="device-details">
          <div class="device-name">${escapeHtml(peer.deviceName)}</div>
          <div class="device-meta">${escapeHtml(peer.osName)} • ${escapeHtml(peer.browserName)}</div>
        </div>
        <span class="badge peer-badge">CONNECTED</span>
      `;
      peerDiv.addEventListener('click', () => {
        targetPeerSelect.value = peer.socketId;
      });
      devicesGrid.appendChild(peerDiv);
    });

    // Update target select dropdown options
    targetPeerSelect.innerHTML = '<option value="all">Broadcast to All Connected Peers</option>';
    otherPeers.forEach(peer => {
      const opt = document.createElement('option');
      opt.value = peer.socketId;
      opt.textContent = `${peer.deviceName} (${peer.osName})`;
      targetPeerSelect.appendChild(opt);
    });

    lucide.createIcons();
  }

  function addFilesToQueue(files) {
    files.forEach(f => fileQueue.push(f));
    renderQueueUI();
  }

  function renderQueueUI() {
    if (fileQueue.length === 0) {
      fileQueueContainer.style.display = 'none';
      return;
    }
    fileQueueContainer.style.display = 'block';
    queueCount.textContent = fileQueue.length;

    queueList.innerHTML = fileQueue.map((file, idx) => `
      <div class="queue-item">
        <div class="queue-file-info">
          <i data-lucide="${getFileIcon(file.name)}"></i>
          <div>
            <div class="queue-file-name">${escapeHtml(file.name)}</div>
            <div class="queue-file-size">${formatBytes(file.size)}</div>
          </div>
        </div>
        <button class="btn btn-sm btn-ghost" onclick="removeQueueItem(${idx})">
          <i data-lucide="x"></i>
        </button>
      </div>
    `).join('');

    lucide.createIcons();
  }

  window.removeQueueItem = (idx) => {
    fileQueue.splice(idx, 1);
    renderQueueUI();
  };

  function createTransferCardUI({ transferId, fileName, fileSize, type, channel }) {
    emptyTransfersState.style.display = 'none';

    const card = document.createElement('div');
    card.className = 'transfer-item';
    card.id = `transfer-${transferId}`;
    card.innerHTML = `
      <div class="transfer-top">
        <div class="transfer-file-meta">
          <i data-lucide="${getFileIcon(fileName)}"></i>
          <div>
            <div class="transfer-file-name">${escapeHtml(fileName)}</div>
            <span class="transfer-channel-tag" id="tag-${transferId}">${channel}</span>
          </div>
        </div>
        <span class="badge ${type === 'sending' ? 'self-badge' : 'peer-badge'}">${type.toUpperCase()}</span>
      </div>
      <div class="transfer-progress-bar-bg">
        <div class="transfer-progress-bar-fill" id="bar-${transferId}"></div>
      </div>
      <div class="transfer-stats-row">
        <span id="pct-${transferId}">0%</span>
        <span id="speed-${transferId}">Calculating speed...</span>
        <span id="size-${transferId}">${formatBytes(fileSize)}</span>
      </div>
    `;

    transfersList.prepend(card);
    lucide.createIcons();
    updateActiveCountUI();
  }

  function updateTransferChannelUI(transferId, channelName) {
    const tag = document.getElementById(`tag-${transferId}`);
    if (tag) tag.textContent = channelName;
  }

  function updateTransferProgressUI(transferId, stats) {
    const bar = document.getElementById(`bar-${transferId}`);
    const pct = document.getElementById(`pct-${transferId}`);
    const speed = document.getElementById(`speed-${transferId}`);
    const tag = document.getElementById(`tag-${transferId}`);

    if (bar) bar.style.width = `${stats.progress}%`;
    if (pct) pct.textContent = `${stats.progress}%`;
    if (tag && stats.channel) tag.textContent = stats.channel;
    if (speed) {
      speed.textContent = `${formatBytes(stats.speedBps)}/s • ETA: ${formatSeconds(stats.etaSec)}`;
    }
  }

  function completeTransferUI(transferId) {
    const card = document.getElementById(`transfer-${transferId}`);
    if (card) {
      card.classList.add('completed');
      const speed = document.getElementById(`speed-${transferId}`);
      const bar = document.getElementById(`bar-${transferId}`);
      const pct = document.getElementById(`pct-${transferId}`);

      if (bar) bar.style.width = '100%';
      if (pct) pct.textContent = '100%';
      if (speed) speed.textContent = 'Completed';
    }
    updateActiveCountUI();
  }

  function errorTransferUI(transferId, errorMsg) {
    const card = document.getElementById(`transfer-${transferId}`);
    if (card) {
      const speed = document.getElementById(`speed-${transferId}`);
      if (speed) speed.textContent = `Error: ${errorMsg}`;
    }
    updateActiveCountUI();
  }

  function updateActiveCountUI() {
    const active = transfersList.querySelectorAll('.transfer-item:not(.completed)').length;
    activeTransfersCount.textContent = `${active} Active`;
  }

  function renderHistoryUI() {
    if (historyItems.length === 0) {
      emptyHistoryState.style.display = 'block';
      historyList.innerHTML = '';
      historyList.appendChild(emptyHistoryState);
      return;
    }

    emptyHistoryState.style.display = 'none';
    historyList.innerHTML = historyItems.map((item, idx) => `
      <div class="history-item">
        <div class="history-file-details">
          <i data-lucide="${getFileIcon(item.name)}"></i>
          <div>
            <div class="history-file-name">${escapeHtml(item.name)}</div>
            <div class="queue-file-size">${formatBytes(item.size)} • ${item.timestamp}</div>
          </div>
        </div>
        <div class="history-actions">
          <button class="btn btn-sm btn-outline" onclick="previewHistoryFile(${idx})">
            <i data-lucide="eye"></i> Preview
          </button>
          <a class="btn btn-sm btn-primary" href="${item.url}" download="${escapeHtml(item.name)}">
            <i data-lucide="download"></i> Download
          </a>
        </div>
      </div>
    `).join('');

    lucide.createIcons();
  }

  window.previewHistoryFile = (idx) => {
    const item = historyItems[idx];
    if (!item) return;

    previewTitle.textContent = item.name;
    previewBody.innerHTML = '';

    if (item.mimeType.startsWith('image/')) {
      const img = document.createElement('img');
      img.src = item.url;
      previewBody.appendChild(img);
    } else if (item.mimeType.startsWith('video/')) {
      const video = document.createElement('video');
      video.src = item.url;
      video.controls = true;
      video.autoplay = true;
      previewBody.appendChild(video);
    } else if (item.mimeType.startsWith('audio/')) {
      const audio = document.createElement('audio');
      audio.src = item.url;
      audio.controls = true;
      audio.autoplay = true;
      previewBody.appendChild(audio);
    } else if (item.mimeType.startsWith('text/') || item.name.endsWith('.txt') || item.name.endsWith('.json') || item.name.endsWith('.md')) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const pre = document.createElement('pre');
        pre.className = 'text-preview';
        pre.textContent = e.target.result;
        previewBody.appendChild(pre);
      };
      reader.readAsText(item.blob);
    } else {
      previewBody.innerHTML = `
        <div class="text-center p-4">
          <i data-lucide="file-question" style="width:64px;height:64px;margin-bottom:12px;"></i>
          <p>Direct preview is not supported for this file type.</p>
          <a class="btn btn-primary mt-3" href="${item.url}" download="${escapeHtml(item.name)}">
            Download File (${formatBytes(item.size)})
          </a>
        </div>
      `;
      lucide.createIcons();
    }

    previewModal.classList.add('active');
  };

  // Fetch Host Local IP & QR Code Data
  async function fetchNetworkInfo() {
    try {
      const res = await fetch('/api/network-info');
      networkInfo = await res.json();
      networkIpText.textContent = `LAN IP: ${networkInfo.localIp}:${networkInfo.port}`;
    } catch (err) {
      networkIpText.textContent = 'Local Network Server';
    }
  }

  async function openQrModal() {
    const targetUrl = `${networkInfo.lanUrl || window.location.origin}?room=${currentRoomId}`;
    qrUrlInput.value = targetUrl;
    qrRoomCodeDisplay.textContent = currentRoomId;

    try {
      const res = await fetch(`/api/qr-code?url=${encodeURIComponent(targetUrl)}`);
      const data = await res.json();
      qrCodeImg.src = data.qrDataUrl;
    } catch (err) {
      console.error('Failed to load QR Code', err);
    }

    qrModal.classList.add('active');
  }

  function checkUrlParamsAndJoinRoom() {
    const urlParams = new URLSearchParams(window.location.search);
    const roomParam = urlParams.get('room');
    if (roomParam && roomParam.length === 6) {
      joinRoom(roomParam);
    } else {
      // Auto-create room if none specified
      btnCreateRoom.click();
    }
  }

  // Utilities
  function detectDeviceDetails() {
    const ua = navigator.userAgent;
    let deviceType = 'desktop';
    if (/Mobi|Android|iPhone|iPad|iPod/i.test(ua)) deviceType = 'mobile';
    if (/iPad|Tablet/i.test(ua)) deviceType = 'tablet';

    let osName = 'Unknown OS';
    if (ua.indexOf('Win') !== -1) osName = 'Windows';
    if (ua.indexOf('Mac') !== -1) osName = 'macOS';
    if (ua.indexOf('Linux') !== -1) osName = 'Linux';
    if (ua.indexOf('Android') !== -1) osName = 'Android';
    if (ua.indexOf('like Mac') !== -1) osName = 'iOS';

    let browserName = 'Browser';
    if (ua.indexOf('Chrome') !== -1) browserName = 'Chrome';
    if (ua.indexOf('Safari') !== -1 && ua.indexOf('Chrome') === -1) browserName = 'Safari';
    if (ua.indexOf('Firefox') !== -1) browserName = 'Firefox';
    if (ua.indexOf('Edg') !== -1) browserName = 'Edge';

    const deviceName = `${osName} ${deviceType === 'mobile' ? 'Phone' : 'Device'}`;
    return { deviceType, osName, browserName, deviceName };
  }

  function getDeviceIcon(type) {
    if (type === 'mobile') return 'smartphone';
    if (type === 'tablet') return 'tablet';
    return 'laptop';
  }

  function getFileIcon(filename) {
    const ext = filename.split('.').pop().toLowerCase();
    if (['jpg', 'jpeg', 'png', 'gif', 'svg', 'webp'].includes(ext)) return 'image';
    if (['mp4', 'mkv', 'avi', 'mov', 'webm'].includes(ext)) return 'film';
    if (['mp3', 'wav', 'flac', 'aac'].includes(ext)) return 'music';
    if (['pdf', 'doc', 'docx', 'txt', 'rtf'].includes(ext)) return 'file-text';
    if (['zip', 'rar', '7z', 'tar', 'gz'].includes(ext)) return 'archive';
    return 'file';
  }

  function formatBytes(bytes, decimals = 2) {
    if (!bytes || bytes === 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
  }

  function formatSeconds(sec) {
    if (!sec || isNaN(sec) || !isFinite(sec)) return '0s';
    const s = Math.round(sec);
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    const remS = s % 60;
    return `${m}m ${remS}s`;
  }

  function generateUuid() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
      const r = Math.random() * 16 | 0;
      const v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }

  function escapeHtml(str) {
    return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  function showToast(msg) {
    let toast = document.createElement('div');
    toast.className = 'toast-notification';
    toast.style.cssText = `
      position: fixed;
      bottom: 24px;
      right: 24px;
      background: rgba(0, 242, 254, 0.95);
      color: #07090e;
      font-weight: 700;
      padding: 12px 24px;
      border-radius: 12px;
      box-shadow: 0 10px 30px rgba(0, 242, 254, 0.4);
      z-index: 2000;
      transition: all 0.3s ease;
    `;
    toast.textContent = msg;
    document.body.appendChild(toast);

    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateY(10px)';
      setTimeout(() => toast.remove(), 300);
    }, 3000);
  }
});
