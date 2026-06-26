const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const cors       = require('cors');
const path       = require('path');

const app    = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  maxHttpBufferSize: 2e6
});

app.use(cors());
app.use(express.json());

// Serve PWA from /public folder (same directory as server.js)
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ── State ──
const channels = new Map();
const users    = new Map();

function getOrCreateChannel(channelId, name) {
  if (!channels.has(channelId)) {
    channels.set(channelId, {
      id: channelId, name: name || channelId,
      members: new Set(), talking: null, history: [],
    });
  }
  return channels.get(channelId);
}

function getChannelInfo(ch) {
  return {
    id:      ch.id,
    name:    ch.name,
    members: [...ch.members].map(sid => users.get(sid)).filter(Boolean),
    talking: ch.talking ? (users.get(ch.talking)?.handle || null) : null,
    history: ch.history,
  };
}

// ── Sockets ──
io.on('connection', (socket) => {
  console.log(`[+] ${socket.id}`);

  socket.on('register', ({ handle, status = 'online', avatar = '' }) => {
    users.set(socket.id, { handle, status, avatar, channels: new Set() });
    socket.emit('registered', { id: socket.id, handle });
  });

  socket.on('join_channel', ({ channelId, channelName }) => {
    const user = users.get(socket.id);
    if (!user) return socket.emit('error', 'Not registered');
    const ch = getOrCreateChannel(channelId, channelName);
    ch.members.add(socket.id);
    user.channels.add(channelId);
    socket.join(channelId);
    socket.emit('channel_joined', getChannelInfo(ch));
    socket.to(channelId).emit('user_joined', { channelId, user: { id: socket.id, handle: user.handle, status: user.status } });
    io.to(channelId).emit('channel_update', getChannelInfo(ch));
  });

  socket.on('leave_channel', ({ channelId }) => leaveChannel(socket, channelId));

  socket.on('ptt_start', ({ channelId }) => {
    const user = users.get(socket.id);
    const ch   = channels.get(channelId);
    if (!user || !ch) return;
    if (ch.talking && ch.talking !== socket.id) {
      return socket.emit('ptt_denied', { reason: 'Channel busy', talker: users.get(ch.talking)?.handle });
    }
    ch.talking = socket.id;
    io.to(channelId).emit('ptt_started', { channelId, userId: socket.id, handle: user.handle });
  });

  socket.on('audio_chunk', ({ channelId, chunk }) => {
    const ch = channels.get(channelId);
    if (!ch || ch.talking !== socket.id) return;
    socket.to(channelId).emit('audio_chunk', { channelId, userId: socket.id, handle: users.get(socket.id)?.handle, chunk });
  });

  socket.on('ptt_stop', ({ channelId, duration, messageId }) => {
    const user = users.get(socket.id);
    const ch   = channels.get(channelId);
    if (!user || !ch || ch.talking !== socket.id) return;
    ch.talking = null;
    const msg = { id: messageId || `${socket.id}-${Date.now()}`, userId: socket.id, handle: user.handle, duration, timestamp: Date.now() };
    ch.history.push(msg);
    if (ch.history.length > 50) ch.history.shift();
    io.to(channelId).emit('ptt_stopped', { channelId, message: msg });
  });

  socket.on('set_status', ({ status }) => {
    const user = users.get(socket.id);
    if (!user) return;
    user.status = status;
    user.channels.forEach(channelId => io.to(channelId).emit('user_status', { channelId, userId: socket.id, handle: user.handle, status }));
  });

  socket.on('list_channels', () => {
    socket.emit('channels_list', [...channels.values()].map(ch => ({ id: ch.id, name: ch.name, members: ch.members.size, talking: ch.talking ? users.get(ch.talking)?.handle : null })));
  });

  socket.on('disconnect', () => {
    const user = users.get(socket.id);
    if (user) { user.channels.forEach(cid => leaveChannel(socket, cid, true)); users.delete(socket.id); }
  });

  function leaveChannel(sock, channelId, isDC = false) {
    const u  = users.get(sock.id);
    const ch = channels.get(channelId);
    if (!ch) return;
    ch.members.delete(sock.id);
    if (u) u.channels.delete(channelId);
    if (ch.talking === sock.id) ch.talking = null;
    sock.leave(channelId);
    if (u) sock.to(channelId).emit('user_left', { channelId, userId: sock.id, handle: u.handle });
    io.to(channelId).emit('channel_update', getChannelInfo(ch));
  }
});

// ── Health ──
app.get('/health', (req, res) => res.json({ status: 'ok', channels: channels.size, users: users.size, uptime: process.uptime() }));

// ── Start ──
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🎙️  10-4 running on port ${PORT}`));
