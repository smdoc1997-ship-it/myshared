/**
 * AirShare Main Application Controller & UI Logic
 */

document.addEventListener('DOMContentLoaded', async () => {
  // Helper to safely call lucide.createIcons without throwing ReferenceError if CDN is delayed
  function safeCreateIcons() {
    if (typeof lucide !== 'undefined' && lucide && lucide.createIcons) {
      try { lucide.createIcons(); } catch (e) {}
    }
  }

  // Initialize Lucide icons safely
  safeCreateIcons();

  // Socket.io connection (Optional on Vercel serverless)
  let socket = null;
  if (typeof io !== 'undefined') {
    try {
      socket = io({
        reconnection: true,
        reconnectionAttempts: Infinity,
        timeout: 3000,
        autoConnect: true
      });
    } catch (e) {
      console.warn('Socket.io client optional:', e);
    }
  }

  // App State
  let currentRoomId = '';
  let selfSocketId = '';
  let networkInfo = { localIp: 'localhost', port: 3000, lanUrl: '' };
  let peers = []; // list of connected peers in current room
  let fileQueue = []; // files selected for upload
  let historyItems = []; // completed transfers
  let html5QrCodeScanner = null; // Camera QR code scanner instance

  // System & Browser Info Detection (with localStorage custom name persistence)
  const deviceInfo = detectDeviceDetails();

  // Fallback incoming transfers storage over Socket.io relay
  const relayIncomingTransfers = new Map();
  const activeRelayOutgoings = new Map();

  // WebRTC Manager Instance
  const webrtcManager = new WebRTCManager(socket, {
    onProgress: (transferId, stats) => updateTransferProgressUI(transferId, stats),
    onComplete: (transferId) => completeTransferUI(transferId),
    onError: (transferId, errorMsg) => errorTransferUI(transferId, errorMsg),
    onFileReceived: (fileData) => handleReceivedFile(fileData),
    onPeerDiscovered: (peerData) => handleDiscoveredPeer(peerData),
    onPeerRenamed: (peerData) => handlePeerRenamed(peerData)
  });

  // DOM Element Selectors
  const networkBadge = document.getElementById('networkBadge');
  const networkIpText = document.getElementById('networkIpText');
  const currentRoomCode = document.getElementById('currentRoomCode');
  const connectionStatusDot = document.getElementById('connectionStatusDot');
  const roomJoinInputGroup = document.getElementById('roomJoinInputGroup');
  const roomCodeInput = document.getElementById('roomCodeInput');
  const btnJoinRoom = document.getElementById('btnJoinRoom');
  const btnCreateRoom = document.getElementById('btnCreateRoom');
  const btnCopyLink = document.getElementById('btnCopyLink');
  const btnLeaveRoom = document.getElementById('btnLeaveRoom');
  const btnShowQr = document.getElementById('btnShowQr');
  const btnOpenScanCamera = document.getElementById('btnOpenScanCamera');
  
  const peerCount = document.getElementById('peerCount');
  const devicesGrid = document.getElementById('devicesGrid');
  const selfDeviceName = document.getElementById('selfDeviceName');
  const selfDeviceMeta = document.getElementById('selfDeviceMeta');
  const btnEditDeviceName = document.getElementById('btnEditDeviceName');

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

  const renameModal = document.getElementById('renameModal');
  const btnCloseRename = document.getElementById('btnCloseRename');
  const btnCancelRename = document.getElementById('btnCancelRename');
  const btnSaveDeviceName = document.getElementById('btnSaveDeviceName');
  const deviceNameInput = document.getElementById('deviceNameInput');

  const cameraQrModal = document.getElementById('cameraQrModal');
  const btnCloseCameraQr = document.getElementById('btnCloseCameraQr');
  const btnStopCameraScan = document.getElementById('btnStopCameraScan');

  // Update Self Device UI
  updateSelfDeviceUI();

  // Restore Saved Transfer History from localStorage
  loadHistoryFromStorage();

  // Fetch Network Info & URL Params asynchronously without blocking DOM initialization
  fetchNetworkInfo().catch(err => console.warn('Network info fetch:', err));
  checkUrlParamsAndJoinRoom();

  // Socket.io Handlers (Persistent auto-reconnect)
  if (socket) {
    socket.on('connect', () => {
      selfSocketId = socket.id;
      connectionStatusDot.classList.add('online');
      console.log('[Socket] Connected with ID:', socket.id);
      if (currentRoomId) {
        joinRoom(currentRoomId);
      }
    });

    socket.on('disconnect', (reason) => {
      console.log('[Socket] Disconnected:', reason);
      if (!webrtcManager.peer) {
        connectionStatusDot.classList.remove('online');
      }
    });

    socket.on('joined-room-success', ({ roomId, selfId, peers: roomPeers }) => {
      currentRoomId = roomId;
      currentRoomCode.textContent = roomId;
      qrRoomCodeDisplay.textContent = roomId;
      connectionStatusDot.classList.add('online');
      localStorage.setItem('airshare_current_room', roomId);
      updatePeersUI(roomPeers);
      updateRoomStateUI(true);
    });

    socket.on('room-peers', (roomPeers) => {
      updatePeersUI(roomPeers);
    });
  }

  // Socket.io Relayed File Transfer Events (Fallback Engine)
  if (socket) {
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
  }

  // UI Event Listeners with Bulletproof Null Guarding
  if (btnJoinRoom) {
    btnJoinRoom.addEventListener('click', () => {
      const code = roomCodeInput ? roomCodeInput.value.trim() : '';
      if (code.length === 6) {
        joinRoom(code);
        showToast(`Joined Room: ${code}`);
      } else {
        alert('Please enter a valid 6-digit room code.');
      }
    });
  }

  if (roomCodeInput) {
    roomCodeInput.addEventListener('keyup', (e) => {
      if (e.key === 'Enter' && btnJoinRoom) {
        btnJoinRoom.click();
      }
    });
  }

  if (btnCreateRoom) {
    btnCreateRoom.addEventListener('click', () => {
      const localCode = Math.floor(100000 + Math.random() * 900000).toString();
      joinRoom(localCode);
      showToast(`New Room Code Created: ${localCode}`);
    });
  }

  if (btnCopyLink) {
    btnCopyLink.addEventListener('click', () => {
      const shareUrl = `${window.location.origin}?room=${currentRoomId}`;
      navigator.clipboard.writeText(shareUrl);
      showToast('Direct invite link copied to clipboard!');
    });
  }

  if (btnLeaveRoom) {
    btnLeaveRoom.addEventListener('click', () => {
      if (socket && socket.connected) {
        socket.emit('leave-room');
      }
      webrtcManager.disconnectAll();
      localStorage.removeItem('airshare_current_room');
      currentRoomId = '';
      if (currentRoomCode) currentRoomCode.textContent = '------';
      if (qrRoomCodeDisplay) qrRoomCodeDisplay.textContent = '------';
      if (connectionStatusDot) connectionStatusDot.classList.remove('online');
      peers = [];
      updatePeersUI([]);
      updateRoomStateUI(false);

      const cleanUrl = window.location.origin + window.location.pathname;
      window.history.pushState({}, '', cleanUrl);

      showToast('Left room and disconnected.');
    });
  }

  if (btnShowQr) {
    btnShowQr.addEventListener('click', () => {
      openQrModal();
    });
  }

  if (btnCloseQr) {
    btnCloseQr.addEventListener('click', () => {
      if (qrModal) qrModal.classList.remove('active');
    });
  }

  if (btnCopyQrUrl) {
    btnCopyQrUrl.addEventListener('click', () => {
      if (qrUrlInput) {
        navigator.clipboard.writeText(qrUrlInput.value);
        showToast('QR Code link copied!');
      }
    });
  }

  if (btnClosePreview) {
    btnClosePreview.addEventListener('click', () => {
      if (previewModal) previewModal.classList.remove('active');
      if (previewBody) previewBody.innerHTML = '';
    });
  }

  // Camera QR Code Scanner Event Handlers
  if (btnOpenScanCamera) {
    btnOpenScanCamera.addEventListener('click', () => startCameraQrScanner());
  }

  if (btnCloseCameraQr) btnCloseCameraQr.addEventListener('click', () => stopCameraQrScanner());
  if (btnStopCameraScan) btnStopCameraScan.addEventListener('click', () => stopCameraQrScanner());

  // Camera QR Scanner Controller
  async function startCameraQrScanner() {
    if (typeof Html5Qrcode === 'undefined') {
      alert('Camera QR scanner library loading. Please try again in a moment.');
      return;
    }

    cameraQrModal.classList.add('active');

    try {
      if (html5QrCodeScanner) {
        await stopCameraQrScanner();
      }

      html5QrCodeScanner = new Html5Qrcode("cameraQrReader");
      await html5QrCodeScanner.start(
        { facingMode: "environment" },
        { fps: 10, qrbox: { width: 250, height: 250 } },
        (decodedText) => {
          console.log('[Camera QR Scan] Decoded:', decodedText);
          let scannedRoomCode = '';
          if (decodedText.includes('room=')) {
            try {
              const urlObj = new URL(decodedText);
              scannedRoomCode = urlObj.searchParams.get('room');
            } catch (e) {
              const match = decodedText.match(/room=(\d{6})/);
              if (match) scannedRoomCode = match[1];
            }
          } else if (decodedText.trim().length === 6 && /^\d+$/.test(decodedText.trim())) {
            scannedRoomCode = decodedText.trim();
          }

          if (scannedRoomCode) {
            stopCameraQrScanner();
            joinRoom(scannedRoomCode);
            showToast(`Joined room ${scannedRoomCode} via Camera QR scan!`);
          }
        },
        () => {}
      );
    } catch (err) {
      console.warn('Camera scanner error:', err);
      showToast('Camera access denied or unavailable.');
    }
  }

  async function stopCameraQrScanner() {
    if (html5QrCodeScanner) {
      try {
        await html5QrCodeScanner.stop();
        html5QrCodeScanner.clear();
      } catch (e) {}
      html5QrCodeScanner = null;
    }
    cameraQrModal.classList.remove('active');
  }

  // Device Rename Event Handlers
  if (btnEditDeviceName) {
    btnEditDeviceName.addEventListener('click', () => {
      deviceNameInput.value = deviceInfo.deviceName;
      renameModal.classList.add('active');
      deviceNameInput.focus();
    });
  }

  const closeRenameModal = () => renameModal.classList.remove('active');
  if (btnCloseRename) btnCloseRename.addEventListener('click', closeRenameModal);
  if (btnCancelRename) btnCancelRename.addEventListener('click', closeRenameModal);

  if (btnSaveDeviceName) {
    btnSaveDeviceName.addEventListener('click', () => {
      const newName = deviceNameInput.value.trim();
      if (!newName) return;
      
      deviceInfo.deviceName = newName;
      localStorage.setItem('airshare_device_name', newName);
      updateSelfDeviceUI();
      
      webrtcManager.updateDeviceName(newName);
      if (socket && socket.connected) {
        socket.emit('join-room', {
          roomId: currentRoomId,
          deviceName: deviceInfo.deviceName,
          deviceType: deviceInfo.deviceType,
          osName: deviceInfo.osName,
          browserName: deviceInfo.browserName
        });
      }
      
      closeRenameModal();
      showToast(`Device renamed to "${newName}"`);
    });
  }

  // File Dropzone Listeners with Null Guards
  if (dropzone) {
    dropzone.addEventListener('click', () => {
      if (fileInput) fileInput.click();
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
      if (dt && dt.files) {
        addFilesToQueue(Array.from(dt.files));
      }
    });
  }

  if (fileInput) {
    fileInput.addEventListener('change', (e) => {
      if (e.target && e.target.files) {
        addFilesToQueue(Array.from(e.target.files));
        fileInput.value = '';
      }
    });
  }

  if (btnClearQueue) {
    btnClearQueue.addEventListener('click', () => {
      fileQueue = [];
      renderQueueUI();
    });
  }

  if (btnSendFiles) {
    btnSendFiles.addEventListener('click', () => {
      if (fileQueue.length === 0) return;
      const targetPeerId = targetPeerSelect ? targetPeerSelect.value : 'all';
      const filesToSend = [...fileQueue];
      fileQueue = [];
      renderQueueUI();

      filesToSend.forEach(file => {
        if (targetPeerId === 'all') {
          const otherPeers = peers.filter(p => p.socketId !== selfSocketId && p.socketId !== webrtcManager.peerId);
          if (otherPeers.length === 0) {
            alert('No other devices connected in this room! Scan QR code on your phone or open another device tab to connect.');
            return;
          }
          otherPeers.forEach(peer => initiateFileTransfer(file, peer.socketId || peer.peerId));
        } else {
          initiateFileTransfer(file, targetPeerId);
        }
      });
    });
  }

  if (btnClearHistory) {
    btnClearHistory.addEventListener('click', () => {
      historyItems = [];
      saveHistoryToStorage();
      renderHistoryUI();
    });
  }

  // Core Transfer Logic
  async function initiateFileTransfer(file, targetSocketId) {
    const transferId = generateUuid();
    
    createTransferCardUI({
      transferId,
      fileName: file.name,
      fileSize: file.size,
      type: 'sending',
      channel: 'WebRTC P2P'
    });

    const p2pSuccess = await webrtcManager.sendFileP2P(targetSocketId, file, transferId);

    if (!p2pSuccess && socket && socket.connected) {
      console.log(`[Transfer] P2P fallback triggered for file ${file.name}. Using Socket.io relay stream.`);
      updateTransferChannelUI(transferId, 'Relayed Stream');
      await sendFileViaSocketRelay(file, targetSocketId, transferId);
    }
  }

  // Socket.io Chunked Relay Implementation
  async function sendFileViaSocketRelay(file, targetSocketId, transferId) {
    activeRelayOutgoings.set(transferId, { cancelled: false });

    socket.emit('relay-file-init', {
      targetSocketId,
      fileMeta: {
        transferId,
        name: file.name,
        size: file.size,
        mimeType: file.type || 'application/octet-stream'
      }
    });

    const CHUNK_SIZE = 256 * 1024; // 256KB chunk size for high-speed streaming
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

      if (chunkIndex % 5 === 0) {
        await new Promise(r => setTimeout(r, 0));
      }
    }

    socket.emit('relay-file-complete', { targetSocketId, fileId: transferId });
    completeTransferUI(transferId);
    activeRelayOutgoings.delete(transferId);
  }

  // Handle Received File
  function handleReceivedFile({ transferId, name, size, mimeType, blob }) {
    const url = URL.createObjectURL(blob);
    
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
    saveHistoryToStorage();
    renderHistoryUI();

    showToast(`Received file: ${name}`);
  }

  function handleDiscoveredPeer(peerData) {
    if (!peers.some(p => p.socketId === peerData.socketId || p.peerId === peerData.peerId)) {
      peers.push(peerData);
      updatePeersUI(peers);
      showToast(`Device connected: ${peerData.deviceName}`);
    }
  }

  function handlePeerRenamed({ peerId, deviceName }) {
    const peer = peers.find(p => p.socketId === peerId || p.peerId === peerId);
    if (peer) {
      peer.deviceName = deviceName;
      updatePeersUI(peers);
      showToast(`Peer renamed to "${deviceName}"`);
    }
  }

  // UI Helpers & Renderers
  function joinRoom(roomId) {
    // If switching from an existing room, disconnect previous peer connections cleanly
    if (currentRoomId && currentRoomId !== roomId) {
      if (socket && socket.connected) {
        socket.emit('leave-room');
      }
      webrtcManager.disconnectAll();
    }

    currentRoomId = roomId;
    if (currentRoomCode) currentRoomCode.textContent = roomId;
    if (qrRoomCodeDisplay) qrRoomCodeDisplay.textContent = roomId;
    if (roomCodeInput) roomCodeInput.value = '';
    if (connectionStatusDot) connectionStatusDot.classList.add('online');
    localStorage.setItem('airshare_current_room', roomId);

    if (socket && socket.connected) {
      socket.emit('join-room', {
        roomId,
        deviceName: deviceInfo.deviceName,
        deviceType: deviceInfo.deviceType,
        osName: deviceInfo.osName,
        browserName: deviceInfo.browserName
      });
    }

    webrtcManager.initPeerJs(roomId, deviceInfo, (peerId) => {
      console.log('[App] PeerJS active with ID:', peerId);
      if (connectionStatusDot) connectionStatusDot.classList.add('online');
    });

    updateRoomStateUI(true);

    const newUrl = new URL(window.location.href);
    newUrl.searchParams.set('room', roomId);
    window.history.pushState({}, '', newUrl);
  }

  function updateRoomStateUI(isJoined) {
    if (roomJoinInputGroup) roomJoinInputGroup.style.display = isJoined ? 'none' : 'inline-flex';
    if (btnLeaveRoom) btnLeaveRoom.style.display = isJoined ? 'inline-flex' : 'none';
    if (btnCopyLink) btnCopyLink.style.display = isJoined ? 'inline-flex' : 'none';
    if (btnShowQr) btnShowQr.style.display = isJoined ? 'inline-flex' : 'none';
  }

  function updateSelfDeviceUI() {
    if (selfDeviceName) selfDeviceName.textContent = `${deviceInfo.deviceName} (You)`;
    if (selfDeviceMeta) selfDeviceMeta.textContent = `${deviceInfo.osName} • ${deviceInfo.browserName}`;
  }

  function updatePeersUI(roomPeers) {
    peers = roomPeers;
    const otherPeers = peers.filter(p => p.socketId !== selfSocketId && p.socketId !== webrtcManager.peerId);
    peerCount.textContent = otherPeers.length + 1;

    devicesGrid.innerHTML = '';

    // Always render Self card first
    const selfDiv = document.createElement('div');
    selfDiv.className = 'device-item self';
    selfDiv.innerHTML = `
      <div class="device-icon"><i data-lucide="${getDeviceIcon(deviceInfo.deviceType)}"></i></div>
      <div class="device-details">
        <div class="device-name-row">
          <span class="device-name">${escapeHtml(deviceInfo.deviceName)} (You)</span>
          <button class="btn-icon-subtle" id="btnEditDeviceNameInner" title="Rename Device">
            <i data-lucide="edit-3"></i>
          </button>
        </div>
        <div class="device-meta">${deviceInfo.osName} • ${deviceInfo.browserName}</div>
      </div>
      <span class="badge self-badge">YOU</span>
    `;
    devicesGrid.appendChild(selfDiv);

    const editBtnInner = document.getElementById('btnEditDeviceNameInner');
    if (editBtnInner) {
      editBtnInner.addEventListener('click', () => {
        deviceNameInput.value = deviceInfo.deviceName;
        renameModal.classList.add('active');
        deviceNameInput.focus();
      });
    }

    // Render connected peers
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
        targetPeerSelect.value = peer.socketId || peer.peerId;
      });
      devicesGrid.appendChild(peerDiv);
    });

    targetPeerSelect.innerHTML = '<option value="all">Broadcast to All Connected Peers</option>';
    otherPeers.forEach(peer => {
      const opt = document.createElement('option');
      opt.value = peer.socketId || peer.peerId;
      opt.textContent = `${peer.deviceName} (${peer.osName})`;
      targetPeerSelect.appendChild(opt);
    });

    safeCreateIcons();
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

    safeCreateIcons();
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
    safeCreateIcons();
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
          ${item.blob || item.url ? `
          <button class="btn btn-sm btn-outline" onclick="previewHistoryFile(${idx})">
            <i data-lucide="eye"></i> Preview
          </button>
          <a class="btn btn-sm btn-primary" href="${item.url || '#'}" download="${escapeHtml(item.name)}">
            <i data-lucide="download"></i> Download
          </a>` : '<span class="queue-file-size">Completed</span>'}
        </div>
      </div>
    `).join('');

    safeCreateIcons();
  }

  function saveHistoryToStorage() {
    try {
      const serializable = historyItems.map(item => ({
        id: item.id,
        name: item.name,
        size: item.size,
        mimeType: item.mimeType,
        timestamp: item.timestamp
      }));
      localStorage.setItem('airshare_history_v3', JSON.stringify(serializable));
    } catch (e) {}
  }

  function loadHistoryFromStorage() {
    try {
      const saved = localStorage.getItem('airshare_history_v3');
      if (saved) {
        historyItems = JSON.parse(saved);
        renderHistoryUI();
      }
    } catch (e) {}
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
      if (item.blob) {
        const reader = new FileReader();
        reader.onload = (e) => {
          const pre = document.createElement('pre');
          pre.className = 'text-preview';
          pre.textContent = e.target.result;
          previewBody.appendChild(pre);
        };
        reader.readAsText(item.blob);
      } else {
        previewBody.innerHTML = `<p class="modal-desc">Text file preview requires file blob in active session.</p>`;
      }
    } else {
      previewBody.innerHTML = `
        <div class="text-center p-4">
          <i data-lucide="file-question" style="width:64px;height:64px;margin-bottom:12px;"></i>
          <p>Direct preview is not supported for this file type.</p>
          <a class="btn btn-primary mt-3" href="${item.url || '#'}" download="${escapeHtml(item.name)}">
            Download File (${formatBytes(item.size)})
          </a>
        </div>
      `;
      safeCreateIcons();
    }

    previewModal.classList.add('active');
  };

  // Fetch Host Local IP & QR Code Data
  async function fetchNetworkInfo() {
    try {
      const res = await fetch('/api/network-info');
      networkInfo = await res.json();
      if (networkInfo.isVercel || window.location.hostname.includes('vercel.app')) {
        networkIpText.textContent = `Vercel Cloud Server`;
      } else if (networkInfo.port) {
        networkIpText.textContent = `LAN IP: ${networkInfo.localIp}:${networkInfo.port}`;
      } else {
        networkIpText.textContent = `Server: ${networkInfo.host || window.location.hostname}`;
      }
    } catch (err) {
      networkIpText.textContent = 'AirShare Online';
    }
  }

  async function openQrModal() {
    const targetUrl = `${networkInfo.lanUrl || window.location.origin}?room=${currentRoomId}`;
    qrUrlInput.value = targetUrl;
    qrRoomCodeDisplay.textContent = currentRoomId;

    // Set immediate client fallback QR image so modal opens instantly
    qrCodeImg.src = `https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(targetUrl)}`;
    qrModal.classList.add('active');

    try {
      const res = await fetch(`/api/qr-code?url=${encodeURIComponent(targetUrl)}`);
      const data = await res.json();
      if (data && data.qrDataUrl) {
        qrCodeImg.src = data.qrDataUrl;
      }
    } catch (err) {
      console.warn('Backend QR API fallback:', err);
    }
  }

  function checkUrlParamsAndJoinRoom() {
    const urlParams = new URLSearchParams(window.location.search);
    const roomParam = urlParams.get('room');

    if (roomParam && roomParam.length === 6) {
      joinRoom(roomParam);
    } else {
      // Start in clean Unjoined State unless explicit room URL parameter is passed
      updateRoomStateUI(false);
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

    const customName = localStorage.getItem('airshare_device_name');
    const deviceName = customName || `${osName} ${deviceType === 'mobile' ? 'Phone' : 'Device'}`;
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

  function showToast(msg, type = 'info') {
    // 1. Toastr.js Integration if loaded
    if (typeof toastr !== 'undefined') {
      try {
        toastr.options = {
          closeButton: true,
          progressBar: true,
          positionClass: "toast-bottom-right",
          timeOut: 3500
        };
        if (type === 'success') toastr.success(msg);
        else if (type === 'error') toastr.error(msg);
        else if (type === 'warning') toastr.warning(msg);
        else toastr.info(msg);
      } catch (e) {}
    }

    // 2. AirShare Custom Glassmorphism Toast Notification
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
      z-index: 9999;
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
