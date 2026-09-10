const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const qrcode = require('qrcode');
const os = require('os');
const path = require('path');
const cors = require('cors');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  },
  maxHttpBufferSize: 1e8 // 100MB for Socket.io chunk fallback
});

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'), {
  etag: false,
  maxAge: 0,
  setHeaders: (res) => {
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  }
}));

// Helper to get local network IPv4 address
function getLocalIpAddress() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const net of interfaces[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        return net.address;
      }
    }
  }
  return 'localhost';
}

const PORT = process.env.PORT || 3000;
const localIp = getLocalIpAddress();

// API Endpoints
app.get('/api/network-info', (req, res) => {
  res.json({
    localIp,
    port: PORT,
    lanUrl: `http://${localIp}:${PORT}`,
    localhostUrl: `http://localhost:${PORT}`
  });
});

// Endpoint to generate QR code data URL
app.get('/api/qr-code', async (req, res) => {
  const targetUrl = req.query.url || `http://${localIp}:${PORT}`;
  try {
    const qrDataUrl = await qrcode.toDataURL(targetUrl, {
      margin: 2,
      width: 320,
      color: {
        dark: '#00f2fe',
        light: '#0b0f19'
      }
    });
    res.json({ qrDataUrl, targetUrl });
  } catch (err) {
    res.status(500).json({ error: 'Failed to generate QR code' });
  }
});

// Generate random 6-digit room code
app.get('/api/room/new', (req, res) => {
  const code = Math.floor(100000 + Math.random() * 900000).toString();
  res.json({ roomId: code });
});

// Socket.io Room & WebRTC Signaling Hub
// roomCode -> Map<socketId, { socketId, deviceName, deviceType, os, browser } >
const rooms = new Map();

io.on('connection', (socket) => {
  let currentRoom = null;
  let peerInfo = null;

  socket.on('join-room', ({ roomId, deviceName, deviceType, osName, browserName }) => {
    if (!roomId) return;

    // Leave previous room if any
    if (currentRoom && rooms.has(currentRoom)) {
      rooms.get(currentRoom).delete(socket.id);
      if (rooms.get(currentRoom).size === 0) {
        rooms.delete(currentRoom);
      } else {
        io.to(currentRoom).emit('peer-left', { socketId: socket.id });
        io.to(currentRoom).emit('room-peers', Array.from(rooms.get(currentRoom).values()));
      }
      socket.leave(currentRoom);
    }

    currentRoom = roomId;
    socket.join(roomId);

    peerInfo = {
      socketId: socket.id,
      deviceName: deviceName || 'Anonymous Device',
      deviceType: deviceType || 'desktop',
      osName: osName || 'Unknown OS',
      browserName: browserName || 'Unknown Browser',
      joinedAt: Date.now()
    };

    if (!rooms.has(roomId)) {
      rooms.set(roomId, new Map());
    }
    rooms.get(roomId).set(socket.id, peerInfo);

    // Notify room of all current peers
    const activePeers = Array.from(rooms.get(roomId).values());
    io.to(roomId).emit('room-peers', activePeers);
    socket.emit('joined-room-success', { roomId, selfId: socket.id, peers: activePeers });

    console.log(`[JOIN] Device ${peerInfo.deviceName} (${socket.id}) joined room ${roomId}`);
  });

  // WebRTC Signaling
  socket.on('webrtc-offer', ({ targetSocketId, offer }) => {
    io.to(targetSocketId).emit('webrtc-offer', {
      senderSocketId: socket.id,
      offer
    });
  });

  socket.on('webrtc-answer', ({ targetSocketId, answer }) => {
    io.to(targetSocketId).emit('webrtc-answer', {
      senderSocketId: socket.id,
      answer
    });
  });

  socket.on('webrtc-ice-candidate', ({ targetSocketId, candidate }) => {
    io.to(targetSocketId).emit('webrtc-ice-candidate', {
      senderSocketId: socket.id,
      candidate
    });
  });

  // Fallback Socket.io Data Relay (Used if WebRTC P2P connection cannot be established)
  socket.on('relay-file-init', ({ targetSocketId, fileMeta }) => {
    io.to(targetSocketId).emit('relay-file-init', {
      senderSocketId: socket.id,
      fileMeta
    });
  });

  socket.on('relay-file-response', ({ targetSocketId, fileId, accepted }) => {
    io.to(targetSocketId).emit('relay-file-response', {
      senderSocketId: socket.id,
      fileId,
      accepted
    });
  });

  socket.on('relay-file-chunk', ({ targetSocketId, fileId, chunkIndex, totalChunks, chunkData }) => {
    io.to(targetSocketId).emit('relay-file-chunk', {
      senderSocketId: socket.id,
      fileId,
      chunkIndex,
      totalChunks,
      chunkData
    });
  });

  socket.on('relay-file-complete', ({ targetSocketId, fileId }) => {
    io.to(targetSocketId).emit('relay-file-complete', {
      senderSocketId: socket.id,
      fileId
    });
  });

  socket.on('relay-file-cancel', ({ targetSocketId, fileId }) => {
    io.to(targetSocketId).emit('relay-file-cancel', {
      senderSocketId: socket.id,
      fileId
    });
  });

  socket.on('leave-room', () => {
    if (currentRoom && rooms.has(currentRoom)) {
      rooms.get(currentRoom).delete(socket.id);
      if (rooms.get(currentRoom).size === 0) {
        rooms.delete(currentRoom);
      } else {
        io.to(currentRoom).emit('peer-left', { socketId: socket.id });
        io.to(currentRoom).emit('room-peers', Array.from(rooms.get(currentRoom).values()));
      }
      socket.leave(currentRoom);
      console.log(`[LEAVE] Socket ${socket.id} left room ${currentRoom}`);
      currentRoom = null;
    }
  });

  // Disconnect handling with 1.5s grace period to allow page refreshes without flickering peers
  socket.on('disconnect', () => {
    const roomToClean = currentRoom;
    const socketIdToClean = socket.id;

    setTimeout(() => {
      if (roomToClean && rooms.has(roomToClean)) {
        const roomMap = rooms.get(roomToClean);
        if (roomMap.has(socketIdToClean)) {
          roomMap.delete(socketIdToClean);
          if (roomMap.size === 0) {
            rooms.delete(roomToClean);
          } else {
            io.to(roomToClean).emit('peer-left', { socketId: socketIdToClean });
            io.to(roomToClean).emit('room-peers', Array.from(roomMap.values()));
          }
        }
      }
      console.log(`[DISCONNECT] Socket ${socketIdToClean} cleaned up from room ${roomToClean}`);
    }, 1500);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n==================================================`);
  console.log(`🚀 AirShare File Sharing App Server Running!`);
  console.log(`--------------------------------------------------`);
  console.log(`🌐 Local Access:    http://localhost:${PORT}`);
  console.log(`📲 Network Access:  http://${localIp}:${PORT}`);
  console.log(`==================================================\n`);
});
