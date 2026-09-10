/**
 * AirShare WebRTC P2P DataChannel & PeerJS Hybrid Connection Manager
 */

class WebRTCManager {
  constructor(socket, options = {}) {
    this.socket = socket;
    this.onTransferInit = options.onTransferInit || (() => {});
    this.onProgress = options.onProgress || (() => {});
    this.onComplete = options.onComplete || (() => {});
    this.onError = options.onError || (() => {});
    this.onFileReceived = options.onFileReceived || (() => {});
    this.onPeerDiscovered = options.onPeerDiscovered || (() => {});
    this.onPeerRenamed = options.onPeerRenamed || (() => {});

    this.peerConnections = new Map(); // targetSocketId -> RTCPeerConnection
    this.dataChannels = new Map();    // targetSocketId -> RTCDataChannel
    this.peerJsConns = new Map();     // peerId -> DataConnection
    this.knownPeerMetas = new Map();  // peerId -> deviceMeta
    this.iceCandidatesQueue = new Map(); // targetSocketId -> Array of RTCIceCandidate
    
    // Track incoming file buffer streams
    this.incomingTransfers = new Map();
    this.activeOutgoingTransfers = new Map();

    this.iceServers = [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' },
      { urls: 'stun:stun2.l.google.com:19302' },
      { urls: 'stun:stun3.l.google.com:19302' },
      { urls: 'stun:stun4.l.google.com:19302' },
      { urls: 'stun:stun.services.mozilla.com:3478' },
      { urls: 'stun:stun.cloudflare.com:3478' },
      { urls: 'stun:global.stun.twilio.com:3478' },
      { urls: 'stun:stun.voipbuster.com:3478' },
      { urls: 'stun:stun.voipstunt.com:3478' },
      {
        urls: 'turn:openrelay.metered.ca:80',
        username: 'openrelayproject',
        credential: 'openrelayproject'
      },
      {
        urls: 'turn:openrelay.metered.ca:443',
        username: 'openrelayproject',
        credential: 'openrelayproject'
      },
      {
        urls: 'turn:openrelay.metered.ca:443?transport=tcp',
        username: 'openrelayproject',
        credential: 'openrelayproject'
      }
    ];

    if (this.socket) {
      this.initSocketListeners();
    }

    this.peer = null;
    this.peerId = '';
    this.deviceMeta = null;

    // Start WebRTC connection persistent keep-alive heartbeat
    this.startHeartbeat();
  }

  // Persistent keep-alive ping loop to prevent connection timeout
  startHeartbeat() {
    setInterval(() => {
      // Ping PeerJS connections
      this.peerJsConns.forEach((conn) => {
        if (conn && conn.open) {
          try { conn.send(JSON.stringify({ type: 'ping' })); } catch (e) {}
        }
      });
      // Ping RTCPeerConnection data channels
      this.dataChannels.forEach((channel) => {
        if (channel && channel.readyState === 'open') {
          try { channel.send(JSON.stringify({ type: 'ping' })); } catch (e) {}
        }
      });
    }, 8000);
  }

  // Initialize PeerJS for Vercel / Cloud Serverless P2P Signaling
  initPeerJs(roomId, deviceMeta, onPeerReady) {
    if (typeof Peer === 'undefined') return;

    this.deviceMeta = deviceMeta;
    const hostId = `airshare-room-${roomId}`;
    const uniqueId = `airshare-room-${roomId}-${Math.random().toString(36).substring(2, 7)}`;

    // Try becoming Room Host first
    this.createPeerInstance(hostId, deviceMeta, onPeerReady, () => {
      // If host ID is taken, register as secondary peer and connect to host!
      console.log('[PeerJS] Host ID taken. Joining room host:', hostId);
      this.createPeerInstance(uniqueId, deviceMeta, onPeerReady, null, hostId);
    });
  }

  createPeerInstance(id, deviceMeta, onPeerReady, onErrorTaken, targetHostId = null) {
    try {
      if (this.peer) {
        this.peer.destroy();
      }

      this.peerId = id;
      this.peer = new Peer(id, {
        debug: 1,
        iceServers: this.iceServers,
        config: {
          iceServers: this.iceServers,
          sdpSemantics: 'unified-plan'
        }
      });

      this.peer.on('open', (assignedId) => {
        console.log('[PeerJS] Registered with ID:', assignedId);
        if (onPeerReady) onPeerReady(assignedId);

        // If secondary peer, connect to host immediately
        if (targetHostId) {
          const conn = this.peer.connect(targetHostId, { reliable: true });
          this.setupPeerJsDataConnection(conn);
        }
      });

      this.peer.on('connection', (conn) => {
        console.log('[PeerJS] Incoming connection from:', conn.peer);
        this.setupPeerJsDataConnection(conn);
      });

      this.peer.on('error', (err) => {
        console.warn('[PeerJS Error]:', err.type, err.message);
        if (err.type === 'unavailable-id' && onErrorTaken) {
          onErrorTaken();
        }
      });
    } catch (err) {
      console.warn('PeerJS init error:', err);
    }
  }

  setupPeerJsDataConnection(conn) {
    conn.on('open', () => {
      this.peerJsConns.set(conn.peer, conn);
      
      // Send handshake with device details
      conn.send(JSON.stringify({
        type: 'peer-handshake',
        peerId: this.peerId,
        deviceMeta: this.deviceMeta
      }));
    });

    conn.on('data', async (data) => {
      if (typeof Blob !== 'undefined' && data instanceof Blob) {
        try {
          data = await data.arrayBuffer();
        } catch (e) {
          console.warn('[PeerJS] Failed to read Blob arrayBuffer:', e);
          return;
        }
      }

      if (typeof data === 'string') {
        try {
          const parsed = JSON.parse(data);
          if (parsed.type === 'ping') {
            return; // keep-alive heartbeat ping acknowledged
          }
          if (parsed.type === 'peer-handshake') {
            const meta = parsed.deviceMeta || {};
            this.knownPeerMetas.set(conn.peer, meta);
            this.onPeerDiscovered({
              socketId: conn.peer,
              peerId: conn.peer,
              deviceId: meta.deviceId || conn.peer,
              deviceName: meta.deviceName || 'Connected Device',
              deviceType: meta.deviceType || 'desktop',
              osName: meta.osName || 'Unknown OS',
              browserName: meta.browserName || 'Browser'
            });

            // Respond back with our own deviceMeta & active room peer list to build full mesh
            if (!parsed.isResponse) {
              try {
                conn.send(JSON.stringify({
                  type: 'peer-handshake',
                  isResponse: true,
                  peerId: this.peerId,
                  deviceMeta: this.deviceMeta,
                  roomPeers: Array.from(this.knownPeerMetas.entries()).map(([pId, pMeta]) => ({
                    peerId: pId,
                    deviceMeta: pMeta
                  }))
                }));
              } catch (e) {}
            }

            // Connect to any un-connected room peers in mesh
            if (Array.isArray(parsed.roomPeers)) {
              parsed.roomPeers.forEach(p => {
                if (p.peerId && p.peerId !== this.peerId && !this.peerJsConns.has(p.peerId) && this.peer) {
                  const meshConn = this.peer.connect(p.peerId, { reliable: true });
                  this.setupPeerJsDataConnection(meshConn);
                }
              });
            }
            return;
          }
          if (parsed.type === 'device-rename') {
            const meta = this.knownPeerMetas.get(conn.peer) || {};
            meta.deviceName = parsed.deviceName;
            this.knownPeerMetas.set(conn.peer, meta);
            this.onPeerRenamed({ peerId: conn.peer, deviceName: parsed.deviceName });
            return;
          }
        } catch (e) {}
      }
      this.handleIncomingDataChannelMessage(conn.peer, data);
    });

    conn.on('close', () => {
      this.peerJsConns.delete(conn.peer);
      this.knownPeerMetas.delete(conn.peer);
    });
  }

  // Broadcast device name change to all connected peers
  updateDeviceName(newDeviceName) {
    if (this.deviceMeta) {
      this.deviceMeta.deviceName = newDeviceName;
    }
    const renamePayload = JSON.stringify({ type: 'device-rename', deviceName: newDeviceName });
    
    this.peerJsConns.forEach((conn) => {
      if (conn && conn.open) {
        try { conn.send(renamePayload); } catch (e) {}
      }
    });

    this.dataChannels.forEach((channel) => {
      if (channel && channel.readyState === 'open') {
        try { channel.send(renamePayload); } catch (e) {}
      }
    });
  }

  // Disconnect from room manually
  disconnectAll() {
    this.peerJsConns.forEach((conn) => {
      try { conn.close(); } catch (e) {}
    });
    this.peerJsConns.clear();

    this.dataChannels.forEach((channel) => {
      try { channel.close(); } catch (e) {}
    });
    this.dataChannels.clear();

    this.peerConnections.forEach((pc) => {
      try { pc.close(); } catch (e) {}
    });
    this.peerConnections.clear();
    this.iceCandidatesQueue.clear();

    if (this.peer) {
      try { this.peer.destroy(); } catch (e) {}
      this.peer = null;
    }
  }

  initSocketListeners() {
    this.socket.on('webrtc-offer', async ({ senderSocketId, offer }) => {
      await this.handleOffer(senderSocketId, offer);
    });

    this.socket.on('webrtc-answer', async ({ senderSocketId, answer }) => {
      await this.handleAnswer(senderSocketId, answer);
    });

    this.socket.on('webrtc-ice-candidate', async ({ senderSocketId, candidate }) => {
      await this.handleIceCandidate(senderSocketId, candidate);
    });
  }

  createPeerConnection(targetSocketId) {
    if (this.peerConnections.has(targetSocketId)) {
      return this.peerConnections.get(targetSocketId);
    }

    const pc = new RTCPeerConnection({
      iceServers: this.iceServers,
      iceCandidatePoolSize: 10
    });

    pc.onicecandidate = (event) => {
      if (event.candidate && this.socket) {
        this.socket.emit('webrtc-ice-candidate', {
          targetSocketId,
          candidate: event.candidate
        });
      }
    };

    pc.ondatachannel = (event) => {
      const channel = event.channel;
      this.setupDataChannelEvents(targetSocketId, channel);
    };

    pc.onconnectionstatechange = () => {
      if (pc.connectionState === 'failed' || pc.connectionState === 'disconnected') {
        this.dataChannels.delete(targetSocketId);
      }
    };

    this.peerConnections.set(targetSocketId, pc);
    return pc;
  }

  // Pre-warm background P2P connection as soon as a peer is discovered in room
  prewarmConnection(targetSocketId) {
    if (!targetSocketId || targetSocketId === this.socket?.id || targetSocketId === this.peerId) return;
    if (this.dataChannels.has(targetSocketId) && this.dataChannels.get(targetSocketId).readyState === 'open') return;
    if (this.peerJsConns.has(targetSocketId)) return;

    // Deterministic offer initiation rule to prevent WebRTC offer glare (collisions):
    // Only initiate offer if selfSocketId < targetSocketId; otherwise wait for remote offer.
    if (this.socket && this.socket.id && this.socket.id > targetSocketId) {
      return;
    }

    this.connectToPeer(targetSocketId).catch(err => {
      console.log('[WebRTC Pre-warm] Background connection attempt:', targetSocketId, err?.message || err);
    });
  }

  // Remove disconnected peer connections cleanly
  removePeer(targetSocketId) {
    if (this.dataChannels.has(targetSocketId)) {
      try { this.dataChannels.get(targetSocketId).close(); } catch (e) {}
      this.dataChannels.delete(targetSocketId);
    }
    if (this.peerConnections.has(targetSocketId)) {
      try { this.peerConnections.get(targetSocketId).close(); } catch (e) {}
      this.peerConnections.delete(targetSocketId);
    }
    if (this.peerJsConns.has(targetSocketId)) {
      try { this.peerJsConns.get(targetSocketId).close(); } catch (e) {}
      this.peerJsConns.delete(targetSocketId);
    }
    this.knownPeerMetas.delete(targetSocketId);
    this.iceCandidatesQueue.delete(targetSocketId);
  }

  // Force reconnect P2P channel to a specific target peer
  forceReconnectPeer(targetSocketId) {
    if (!targetSocketId) return;
    console.log('[WebRTC] Forcing P2P reconnect to peer:', targetSocketId);
    this.removePeer(targetSocketId);
    return this.connectToPeer(targetSocketId);
  }

  async connectToPeer(targetSocketId) {
    if (this.peerJsConns.has(targetSocketId)) {
      return this.peerJsConns.get(targetSocketId);
    }

    if (targetSocketId.startsWith('airshare-') && this.peer) {
      const conn = this.peer.connect(targetSocketId, { reliable: true });
      return new Promise((resolve) => {
        conn.on('open', () => {
          this.setupPeerJsDataConnection(conn);
          resolve(conn);
        });
        setTimeout(() => resolve(null), 4000);
      });
    }

    if (this.dataChannels.has(targetSocketId) && this.dataChannels.get(targetSocketId).readyState === 'open') {
      return this.dataChannels.get(targetSocketId);
    }

    if (!this.socket) return null;

    const pc = this.createPeerConnection(targetSocketId);
    const dataChannel = pc.createDataChannel('airshare-file-transfer', { ordered: true });

    this.setupDataChannelEvents(targetSocketId, dataChannel);

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);

    this.socket.emit('webrtc-offer', { targetSocketId, offer });

    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        if (dataChannel.readyState !== 'open') {
          resolve(null);
        }
      }, 4000);

      dataChannel.onopen = () => {
        clearTimeout(timeout);
        resolve(dataChannel);
      };
    });
  }

  async handleOffer(senderSocketId, offer) {
    const pc = this.createPeerConnection(senderSocketId);

    // Perfect Negotiation rollback on offer collision
    if (pc.signalingState !== 'stable') {
      try {
        await pc.setLocalDescription({ type: 'rollback' });
      } catch (e) {}
    }

    await pc.setRemoteDescription(new RTCSessionDescription(offer));
    await this.drainIceCandidateQueue(senderSocketId, pc);

    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);

    if (this.socket) {
      this.socket.emit('webrtc-answer', {
        targetSocketId: senderSocketId,
        answer
      });
    }
  }

  async handleAnswer(senderSocketId, answer) {
    const pc = this.peerConnections.get(senderSocketId);
    if (pc) {
      await pc.setRemoteDescription(new RTCSessionDescription(answer));
      await this.drainIceCandidateQueue(senderSocketId, pc);
    }
  }

  async handleIceCandidate(senderSocketId, candidate) {
    const pc = this.peerConnections.get(senderSocketId);
    if (pc && pc.remoteDescription && pc.remoteDescription.type) {
      try {
        await pc.addIceCandidate(new RTCIceCandidate(candidate));
      } catch (e) {
        console.warn(`[ICE Candidate Error] ${senderSocketId}:`, e);
      }
    } else {
      if (!this.iceCandidatesQueue.has(senderSocketId)) {
        this.iceCandidatesQueue.set(senderSocketId, []);
      }
      this.iceCandidatesQueue.get(senderSocketId).push(candidate);
    }
  }

  async drainIceCandidateQueue(senderSocketId, pc) {
    const queue = this.iceCandidatesQueue.get(senderSocketId);
    if (queue && queue.length > 0) {
      while (queue.length > 0) {
        const candidate = queue.shift();
        try {
          await pc.addIceCandidate(new RTCIceCandidate(candidate));
        } catch (e) {
          console.warn(`[Drain ICE Error] ${senderSocketId}:`, e);
        }
      }
      this.iceCandidatesQueue.delete(senderSocketId);
    }
  }

  setupDataChannelEvents(targetSocketId, channel) {
    channel.binaryType = 'arraybuffer';
    channel.bufferedAmountLowThreshold = 256 * 1024;
    this.dataChannels.set(targetSocketId, channel);

    const sendHandshake = () => {
      try {
        if (channel.readyState === 'open') {
          channel.send(JSON.stringify({
            type: 'peer-handshake',
            peerId: this.socket?.id || this.peerId,
            deviceMeta: this.deviceMeta
          }));
        }
      } catch (e) {}
    };

    if (channel.readyState === 'open') {
      sendHandshake();
    } else {
      channel.onopen = () => {
        sendHandshake();
      };
    }

    channel.onmessage = (event) => {
      this.handleIncomingDataChannelMessage(targetSocketId, event.data);
    };

    channel.onerror = (err) => {
      console.error(`[WebRTC Error] DataChannel ${targetSocketId}:`, err);
    };

    channel.onclose = () => {
      this.dataChannels.delete(targetSocketId);
    };
  }

  async handleIncomingDataChannelMessage(senderSocketId, data) {
    if (typeof Blob !== 'undefined' && data instanceof Blob) {
      try {
        data = await data.arrayBuffer();
      } catch (e) {
        console.warn(`[WebRTC] Failed to convert Blob payload from ${senderSocketId}:`, e);
        return;
      }
    }

    if (typeof data === 'string') {
      try {
        const msg = JSON.parse(data);
        if (msg.type === 'ping') return;
        if (msg.type === 'peer-handshake') {
          const meta = msg.deviceMeta || {};
          this.knownPeerMetas.set(senderSocketId, meta);
          this.onPeerDiscovered({
            socketId: senderSocketId,
            peerId: senderSocketId,
            deviceId: meta.deviceId || senderSocketId,
            deviceName: meta.deviceName || 'Connected Device',
            deviceType: meta.deviceType || 'desktop',
            osName: meta.osName || 'Unknown OS',
            browserName: meta.browserName || 'Browser'
          });
          return;
        }
        if (msg.type === 'device-rename') {
          this.onPeerRenamed({ peerId: senderSocketId, deviceName: msg.deviceName });
          return;
        }
        if (msg.type === 'file-header') {
          this.incomingTransfers.set(msg.transferId, {
            senderSocketId,
            metadata: msg,
            chunks: [],
            receivedBytes: 0,
            totalBytes: msg.size,
            startTime: Date.now()
          });

          // Trigger receiver UI transfer card creation immediately
          this.onTransferInit({
            transferId: msg.transferId,
            fileName: msg.name,
            fileSize: msg.size,
            type: 'receiving',
            channel: 'WebRTC Direct P2P'
          });
        } else if (msg.type === 'file-cancel') {
          const transfer = this.incomingTransfers.get(msg.transferId);
          if (transfer) {
            this.incomingTransfers.delete(msg.transferId);
            this.onError(msg.transferId, 'Transfer cancelled by sender');
          }
        } else if (msg.type === 'file-ack') {
          // Sender receives confirmation that receiver assembled full file
          this.onComplete(msg.transferId);
        }
      } catch (err) {
        console.error('Failed to parse WebRTC text message:', err);
      }
    } else if (data instanceof ArrayBuffer || data instanceof Uint8Array) {
      const arrayBuffer = data instanceof Uint8Array ? data.buffer : data;
      const headerLength = 36;
      const textDecoder = new TextDecoder();
      const transferId = textDecoder.decode(arrayBuffer.slice(0, headerLength)).trim();
      const chunkData = arrayBuffer.slice(headerLength);

      const transfer = this.incomingTransfers.get(transferId);
      if (!transfer) return;

      transfer.chunks.push(chunkData);
      transfer.receivedBytes += chunkData.byteLength;

      const progress = Math.min(100, Math.round((transfer.receivedBytes / transfer.totalBytes) * 100));
      const elapsedSec = (Date.now() - transfer.startTime) / 1000;
      const speedBps = elapsedSec > 0 ? transfer.receivedBytes / elapsedSec : 0;
      const remainingBytes = transfer.totalBytes - transfer.receivedBytes;
      const etaSec = speedBps > 0 ? remainingBytes / speedBps : 0;

      this.onProgress(transferId, {
        progress,
        receivedBytes: transfer.receivedBytes,
        totalBytes: transfer.totalBytes,
        speedBps,
        etaSec,
        channel: 'WebRTC Direct P2P'
      });

      if (transfer.receivedBytes >= transfer.totalBytes) {
        const fileBlob = new Blob(transfer.chunks, { type: transfer.metadata.mimeType || 'application/octet-stream' });
        this.incomingTransfers.delete(transferId);

        // Mark receiver card complete
        this.onComplete(transferId);

        // Send confirmation ACK back to sender over DataChannel
        const ackMsg = JSON.stringify({ type: 'file-ack', transferId });
        try {
          if (this.peerJsConns.has(senderSocketId)) {
            this.peerJsConns.get(senderSocketId).send(ackMsg);
          } else if (this.dataChannels.has(senderSocketId)) {
            this.dataChannels.get(senderSocketId).send(ackMsg);
          }
        } catch (e) {}

        this.onFileReceived({
          transferId,
          name: transfer.metadata.name,
          size: transfer.metadata.size,
          mimeType: transfer.metadata.mimeType,
          blob: fileBlob,
          senderSocketId
        });
      }
    }
  }

  async sendFileP2P(targetSocketId, file, transferId) {
    const dataChannel = await this.connectToPeer(targetSocketId);
    if (!dataChannel) {
      return false;
    }

    // Extract raw RTCDataChannel underlying PeerJS wrapper if present
    const rawChannel = dataChannel._channel || dataChannel.dataChannel || dataChannel;

    this.activeOutgoingTransfers.set(transferId, { paused: false, cancelled: false });

    const headerMsg = JSON.stringify({
      type: 'file-header',
      transferId,
      name: file.name,
      size: file.size,
      mimeType: file.type || 'application/octet-stream'
    });

    try {
      if (dataChannel.send) dataChannel.send(headerMsg);
      else if (rawChannel.send) rawChannel.send(headerMsg);
    } catch (e) {
      console.warn('Failed to send file header:', e);
      return false;
    }

    const CHUNK_SIZE = 256 * 1024; // 256KB high-speed streaming chunk size
    const HIGH_WATERMARK = 4 * 1024 * 1024; // 4MB buffer high watermark
    const LOW_WATERMARK = 2 * 1024 * 1024; // 2MB buffer low watermark

    if (rawChannel && rawChannel.bufferedAmountLowThreshold !== undefined) {
      try {
        rawChannel.bufferedAmountLowThreshold = LOW_WATERMARK;
      } catch (e) {}
    }

    const encoder = new TextEncoder();
    const paddedId = transferId.padEnd(36, ' ');
    const idHeaderBytes = encoder.encode(paddedId); // 36 bytes fixed

    let offset = 0;
    const startTime = Date.now();

    while (offset < file.size) {
      const state = this.activeOutgoingTransfers.get(transferId);
      if (!state || state.cancelled) {
        const cancelMsg = JSON.stringify({ type: 'file-cancel', transferId });
        try {
          if (dataChannel.send) dataChannel.send(cancelMsg);
          else if (rawChannel.send) rawChannel.send(cancelMsg);
        } catch (e) {}
        return true;
      }

      // Pause loop check
      while (state.paused) {
        await new Promise(r => setTimeout(r, 200));
        if (!this.activeOutgoingTransfers.has(transferId) || state.cancelled) break;
      }

      // Backpressure control for maximum throughput without overflow
      if (rawChannel && typeof rawChannel.bufferedAmount === 'number' && rawChannel.bufferedAmount > HIGH_WATERMARK) {
        await new Promise((resolve) => {
          const timeout = setTimeout(() => {
            if (rawChannel) rawChannel.onbufferedamountlow = null;
            resolve();
          }, 150);

          rawChannel.onbufferedamountlow = () => {
            clearTimeout(timeout);
            rawChannel.onbufferedamountlow = null;
            resolve();
          };
        });
      }

      const slice = file.slice(offset, offset + CHUNK_SIZE);
      const chunkBuffer = await slice.arrayBuffer();

      const combinedBuffer = new Uint8Array(36 + chunkBuffer.byteLength);
      combinedBuffer.set(idHeaderBytes, 0);
      combinedBuffer.set(new Uint8Array(chunkBuffer), 36);

      try {
        if (dataChannel.send) {
          dataChannel.send(combinedBuffer.buffer);
        } else if (rawChannel.send) {
          rawChannel.send(combinedBuffer.buffer);
        }
      } catch (err) {
        console.warn('DataChannel send error, backing off:', err);
        await new Promise(r => setTimeout(r, 20));
      }

      offset += chunkBuffer.byteLength;

      const progress = Math.min(100, Math.round((offset / file.size) * 100));
      const elapsedSec = (Date.now() - startTime) / 1000;
      const speedBps = elapsedSec > 0 ? offset / elapsedSec : 0;
      const remainingBytes = file.size - offset;
      const etaSec = speedBps > 0 ? remainingBytes / speedBps : 0;

      this.onProgress(transferId, {
        progress,
        sentBytes: offset,
        totalBytes: file.size,
        speedBps,
        etaSec,
        channel: 'WebRTC Direct P2P'
      });
    }

    this.onComplete(transferId);
    this.activeOutgoingTransfers.delete(transferId);
    return true;
  }

  pauseTransfer(transferId) {
    const state = this.activeOutgoingTransfers.get(transferId);
    if (state) state.paused = true;
  }

  resumeTransfer(transferId) {
    const state = this.activeOutgoingTransfers.get(transferId);
    if (state) state.paused = false;
  }

  cancelTransfer(transferId) {
    const state = this.activeOutgoingTransfers.get(transferId);
    if (state) {
      state.cancelled = true;
      this.activeOutgoingTransfers.delete(transferId);
    }
  }
}
