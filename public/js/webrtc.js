/**
 * AirShare WebRTC P2P DataChannel & PeerJS Hybrid Connection Manager
 */

class WebRTCManager {
  constructor(socket, options = {}) {
    this.socket = socket;
    this.onProgress = options.onProgress || (() => {});
    this.onComplete = options.onComplete || (() => {});
    this.onError = options.onError || (() => {});
    this.onFileReceived = options.onFileReceived || (() => {});
    this.onPeerDiscovered = options.onPeerDiscovered || (() => {});

    this.peerConnections = new Map(); // targetSocketId -> RTCPeerConnection
    this.dataChannels = new Map();    // targetSocketId -> RTCDataChannel
    this.peerJsConns = new Map();     // peerId -> DataConnection
    this.knownPeerMetas = new Map();  // peerId -> deviceMeta
    
    // Track incoming file buffer streams
    this.incomingTransfers = new Map();
    this.activeOutgoingTransfers = new Map();

    this.iceServers = [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' },
      { urls: 'stun:stun2.l.google.com:19302' }
    ];

    if (this.socket) {
      this.initSocketListeners();
    }

    this.peer = null;
    this.peerId = '';
    this.deviceMeta = null;
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
        config: { iceServers: this.iceServers }
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

    conn.on('data', (data) => {
      if (typeof data === 'string') {
        try {
          const parsed = JSON.parse(data);
          if (parsed.type === 'peer-handshake') {
            this.knownPeerMetas.set(conn.peer, parsed.deviceMeta);
            this.onPeerDiscovered({
              socketId: conn.peer,
              peerId: conn.peer,
              deviceName: parsed.deviceMeta.deviceName,
              deviceType: parsed.deviceMeta.deviceType,
              osName: parsed.deviceMeta.osName,
              browserName: parsed.deviceMeta.browserName
            });
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

    const pc = new RTCPeerConnection({ iceServers: this.iceServers });

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

  async connectToPeer(targetSocketId) {
    // 1. Check if existing PeerJS connection open
    if (this.peerJsConns.has(targetSocketId)) {
      return this.peerJsConns.get(targetSocketId);
    }

    // 2. Check if direct PeerJS ID
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

    // 3. WebRTC DataChannel via Socket.io
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
    await pc.setRemoteDescription(new RTCSessionDescription(offer));
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
    }
  }

  async handleIceCandidate(senderSocketId, candidate) {
    const pc = this.peerConnections.get(senderSocketId);
    if (pc && pc.remoteDescription) {
      await pc.addIceCandidate(new RTCIceCandidate(candidate));
    }
  }

  setupDataChannelEvents(targetSocketId, channel) {
    channel.binaryType = 'arraybuffer';
    channel.bufferedAmountLowThreshold = 256 * 1024;
    this.dataChannels.set(targetSocketId, channel);

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

  handleIncomingDataChannelMessage(senderSocketId, data) {
    if (typeof data === 'string') {
      try {
        const msg = JSON.parse(data);
        if (msg.type === 'file-header') {
          this.incomingTransfers.set(msg.transferId, {
            senderSocketId,
            metadata: msg,
            chunks: [],
            receivedBytes: 0,
            totalBytes: msg.size,
            startTime: Date.now()
          });
        } else if (msg.type === 'file-cancel') {
          const transfer = this.incomingTransfers.get(msg.transferId);
          if (transfer) {
            this.incomingTransfers.delete(msg.transferId);
            this.onError(msg.transferId, 'Transfer cancelled by sender');
          }
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
    if (!dataChannel || (dataChannel.readyState && dataChannel.readyState !== 'open' && !dataChannel.send)) {
      return false;
    }

    this.activeOutgoingTransfers.set(transferId, { cancelled: false });

    const headerMsg = JSON.stringify({
      type: 'file-header',
      transferId,
      name: file.name,
      size: file.size,
      mimeType: file.type || 'application/octet-stream'
    });

    dataChannel.send(headerMsg);

    const CHUNK_SIZE = 64 * 1024;
    const encoder = new TextEncoder();
    const paddedId = transferId.padEnd(36, ' ');
    const idHeaderBytes = encoder.encode(paddedId);

    let offset = 0;
    const startTime = Date.now();

    while (offset < file.size) {
      const state = this.activeOutgoingTransfers.get(transferId);
      if (!state || state.cancelled) {
        dataChannel.send(JSON.stringify({ type: 'file-cancel', transferId }));
        return true;
      }

      if (dataChannel.bufferedAmount && dataChannel.bufferedAmount > dataChannel.bufferedAmountLowThreshold) {
        await new Promise((resolve) => {
          dataChannel.onbufferedamountlow = () => {
            dataChannel.onbufferedamountlow = null;
            resolve();
          };
        });
      }

      const slice = file.slice(offset, offset + CHUNK_SIZE);
      const chunkBuffer = await slice.arrayBuffer();

      const combinedBuffer = new Uint8Array(idHeaderBytes.length + chunkBuffer.byteLength);
      combinedBuffer.set(idHeaderBytes, 0);
      combinedBuffer.set(new Uint8Array(chunkBuffer), idHeaderBytes.length);

      dataChannel.send(combinedBuffer.buffer);

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

  cancelTransfer(transferId) {
    if (this.activeOutgoingTransfers.has(transferId)) {
      this.activeOutgoingTransfers.get(transferId).cancelled = true;
    }
  }
}
