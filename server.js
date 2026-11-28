"use strict";

/**
 * KiroApp Watch Party - realtime WebSocket server
 *
 * Host controls (play / pause / seek / rate / change_video) are applied to an
 * authoritative room state on the server and broadcast to every member, so
 * everyone stays in sync.
 */

const http = require("http");
const crypto = require("crypto");
const { WebSocketServer, WebSocket } = require("ws");

// ---------------------------------------------------------------- config ---
const PORT = parseInt(process.env.PORT || "8080", 10);
const WS_TOKEN = process.env.WS_TOKEN || ""; // optional shared secret (?token=...)
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean); // empty = allow all. Native apps send no Origin header anyway.
const MAX_ROOM_SIZE = parseInt(process.env.MAX_ROOM_SIZE || "50", 10);
const SMALL_MESSAGE_BYTES = 24 * 1024; // video set-up notes (SDP) and the host's stream note are bigger than chat lines
const MAX_MESSAGE_BYTES = 320 * 1024; // only a community chat photo (a small JPEG) is allowed to be this large
const HEARTBEAT_MS = 30_000;
const EMPTY_ROOM_TTL_MS = 10 * 60_000; // end the room after everyone has been offline this long
const VOICE_SIGNAL_MAX = 16000;
const VOICE_GRACE_MS = 10_000; // keep a disconnected member in the voice call this long
const HOST_GRACE_MS = 15_000; // wait before migrating host after disconnect
const RATE_LIMIT_PER_SEC = 20;
const CHAT_MAX_LEN = 500;
const NAME_MAX_LEN = 32;
const COMMUNITY_TEXT_MAX = 1000;
const COMMUNITY_IMAGE_MAX = 240_000; // characters of the base64 photo (about 175 KB)
const COMMUNITY_MIN_GAP_MS = 600;
const CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

// ----------------------------------------------------------------- state ---
/** @type {Map<string, Room>} */
const rooms = new Map();

/**
 * @typedef {Object} Member
 * @property {string} userId
 * @property {string} name
 * @property {WebSocket|null} ws
 * @property {number} joinedAt
 * @property {number|null} disconnectedAt
 *
 * @typedef {Object} Room
 * @property {string} id
 * @property {string} hostId
 * @property {Map<string, Member>} members
 * @property {{url:string,title:string,episode:string,meta:object}|null} video
 * @property {{playing:boolean,position:number,rate:number,updatedAt:number}} playback
 * @property {NodeJS.Timeout|null} cleanupTimer
 * @property {NodeJS.Timeout|null} hostTimer
 */

function newRoomCode() {
  for (let attempt = 0; attempt < 20; attempt++) {
    let code = "";
    const bytes = crypto.randomBytes(6);
    for (let i = 0; i < 6; i++) code += CODE_ALPHABET[bytes[i] % CODE_ALPHABET.length];
    if (!rooms.has(code)) return code;
  }
  throw new Error("could not allocate room code");
}

const now = () => Date.now();

function currentPosition(room) {
  const p = room.playback;
  if (!p.playing) return p.position;
  return p.position + ((now() - p.updatedAt) / 1000) * p.rate;
}

function playbackSnapshot(room, by) {
  const p = room.playback;
  return {
    playing: p.playing,
    position: currentPosition(room),
    rate: p.rate,
    video: room.video,
    settings: room.settings,
    by: by || null,
    serverTime: now(),
  };
}

function memberList(room) {
  return [...room.members.values()].map((m) => ({
    userId: m.userId,
    name: m.name,
    isHost: m.userId === room.hostId,
    online: !!m.ws,
  }));
}

/** Who is in the voice call and whether they are muted / deafened (speaking is relayed, not stored). */
function voiceList(room) {
  return [...room.members.values()]
    .filter((m) => m.voice)
    .map((m) => ({ userId: m.userId, muted: m.voice.muted, deafened: m.voice.deafened }));
}

function broadcastVoice(room) {
  broadcast(room, "voice_state", { members: voiceList(room) });
}

function leaveVoice(room, member) {
  if (member.voiceTimer) {
    clearTimeout(member.voiceTimer);
    member.voiceTimer = null;
  }
  if (!member.voice) return;
  member.voice = null;
  broadcastVoice(room);
}

// --------------------------------------------------------------- helpers ---
function send(ws, type, payload = {}) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type, ...payload }));
  }
}

function broadcast(room, type, payload = {}, exceptUserId = null) {
  const data = JSON.stringify({ type, ...payload });
  for (const m of room.members.values()) {
    if (m.userId === exceptUserId) continue;
    if (m.ws && m.ws.readyState === WebSocket.OPEN) m.ws.send(data);
  }
}

function sendError(ws, code, message) {
  send(ws, "error", { code, message });
}

function cleanString(v, max) {
  if (typeof v !== "string") return "";
  // strip control characters, trim, cap length
  return v.replace(/[\u0000-\u001f\u007f]/g, "").trim().slice(0, max);
}

function cleanNumber(v, min, max, fallback) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

/** What the host is using (source, audio, subtitle, fullscreen, speed): guests copy it. */
function cleanSettings(v) {
  const o = v && typeof v === "object" && !Array.isArray(v) ? v : {};
  return {
    provider: cleanString(o.provider, 80),
    audio: cleanString(o.audio, 20),
    sub: cleanString(o.sub, 20),
    fullscreen: o.fullscreen === true,
    speed: cleanNumber(o.speed, 0.25, 4, 1),
    // subtitle look: font name and indexes into the app's size / background / edge lists (-1 = not set)
    subFont: cleanString(o.subFont, 40),
    subSize: Math.round(cleanNumber(o.subSize, -1, 9, -1)),
    subBg: Math.round(cleanNumber(o.subBg, -1, 9, -1)),
    subEdge: Math.round(cleanNumber(o.subEdge, -1, 9, -1)),
    // the exact stream the host plays (a small JSON note), so guests play the very same one
    src: cleanString(o.src, 6000),
  };
}

function cleanBrowse(v) {
  if (!v || typeof v !== "object") return null;
  return {
    screen: cleanString(v.screen, 24),
    animeId: cleanNumber(v.animeId, 0, 1e9, 0),
    title: cleanString(v.title, 120),
    subtitle: cleanString(v.subtitle, 160),
    poster: cleanString(v.poster, 400),
    banner: cleanString(v.banner, 400),
    query: cleanString(v.query, 80),
  };
}

function cleanVideo(v) {
  if (!v || typeof v !== "object") return null;
  const url = cleanString(v.url, 2048);
  if (url && !/^https?:\/\//i.test(url)) return null;
  const meta = v.meta && typeof v.meta === "object" && !Array.isArray(v.meta) ? v.meta : {};
  let metaStr = "{}";
  try {
    metaStr = JSON.stringify(meta);
  } catch (_) {}
  return {
    url,
    title: cleanString(v.title, 200),
    episode: cleanString(String(v.episode ?? ""), 50),
    meta: metaStr.length <= 2048 ? JSON.parse(metaStr) : {},
  };
}

// ----------------------------------------------------------- room logic ---
function createRoom(host) {
  const room = {
    id: newRoomCode(),
    hostId: host.userId,
    members: new Map([[host.userId, host]]),
    video: null,
    browse: null,
    settings: cleanSettings(null),
    playback: { playing: false, position: 0, rate: 1, updatedAt: now() },
    cleanupTimer: null,
    hostTimer: null,
  };
  rooms.set(room.id, room);
  return room;
}

function scheduleCleanupIfEmpty(room) {
  const anyOnline = [...room.members.values()].some((m) => m.ws);
  if (anyOnline) {
    if (room.cleanupTimer) {
      clearTimeout(room.cleanupTimer);
      room.cleanupTimer = null;
    }
    return;
  }
  if (room.cleanupTimer) return;
  room.cleanupTimer = setTimeout(() => {
    const stillEmpty = ![...room.members.values()].some((m) => m.ws);
    if (stillEmpty) rooms.delete(room.id);
    else room.cleanupTimer = null;
  }, EMPTY_ROOM_TTL_MS);
}

function migrateHost(room) {
  const candidates = [...room.members.values()]
    .filter((m) => m.ws)
    .sort((a, b) => a.joinedAt - b.joinedAt);
  if (!candidates.length) return;
  room.hostId = candidates[0].userId;
  broadcast(room, "host_changed", { hostId: room.hostId, members: memberList(room) });
}

function removeMember(room, userId, { reason } = {}) {
  const m = room.members.get(userId);
  if (!m) return;
  const hadVoice = !!m.voice;
  if (m.voiceTimer) clearTimeout(m.voiceTimer);
  m.voice = null;
  room.members.delete(userId);
  if (hadVoice) broadcastVoice(room);
  if (m.ws) {
    m.ws.roomId = null;
    if (reason) send(m.ws, reason, {});
  }
  broadcast(room, "member_left", { userId, name: m.name, members: memberList(room) });
  if (room.hostId === userId) migrateHost(room);
  if (room.members.size === 0) rooms.delete(room.id);
  else scheduleCleanupIfEmpty(room);
}

function handleDisconnect(ws) {
  const room = ws.roomId ? rooms.get(ws.roomId) : null;
  if (!room) return;
  const m = room.members.get(ws.userId);
  if (!m || m.ws !== ws) return; // already replaced by a newer connection
  m.ws = null;
  m.disconnectedAt = now();
  broadcast(room, "member_offline", { userId: m.userId, name: m.name, members: memberList(room) });
  if (m.voice) {
    // keep the voice slot for a few seconds so a network blip does not drop the call for everyone
    if (m.voiceTimer) clearTimeout(m.voiceTimer);
    m.voiceTimer = setTimeout(() => {
      m.voiceTimer = null;
      if (!m.ws) leaveVoice(room, m);
    }, VOICE_GRACE_MS);
  }

  if (room.hostId === m.userId) {
    // give the host a short window to reconnect before handing over control
    if (room.hostTimer) clearTimeout(room.hostTimer);
    room.hostTimer = setTimeout(() => {
      room.hostTimer = null;
      const host = room.members.get(room.hostId);
      if (host && !host.ws) migrateHost(room);
    }, HOST_GRACE_MS);
  }
  scheduleCleanupIfEmpty(room);
}

function attachMember(ws, room, member) {
  member.ws = ws;
  member.disconnectedAt = null;
  if (member.voiceTimer) {
    clearTimeout(member.voiceTimer);
    member.voiceTimer = null;
  }
  ws.roomId = room.id;
  ws.userId = member.userId;
  if (room.hostTimer && room.hostId === member.userId) {
    clearTimeout(room.hostTimer);
    room.hostTimer = null;
  }
  if (room.cleanupTimer) {
    clearTimeout(room.cleanupTimer);
    room.cleanupTimer = null;
  }
}

function joinedPayload(room, member) {
  return {
    roomId: room.id,
    you: { userId: member.userId, name: member.name, isHost: room.hostId === member.userId },
    hostId: room.hostId,
    members: memberList(room),
    voice: voiceList(room),
    browse: room.browse,
    state: playbackSnapshot(room),
  };
}

// ------------------------------------------------- live announcements ---
// The developer sends one message to every phone that is open, as a short fading prompt with a
// verified name. Phones answer with a like or a dislike; the totals are pushed back to everyone.
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || ""; // secret that lets the developer announce; empty = announcing is off
const ANNOUNCE_LATE_JOIN_MS = 90_000; // a phone that connects this soon after still sees it
const ANNOUNCE_MIN_MS = 5_000;
const ANNOUNCE_MAX_MS = 30_000;
const ANNOUNCE_TEXT_MAX = 240;
const COUNTS_PUSH_MS = 400; // reactions are batched so a busy announcement does not flood every phone

/** @type {Set<WebSocket>} phones that asked for announcements (the app, while it is open) */
const globals = new Set();
/** @type {{id:string,text:string,username:string,avatar:string,createdAt:number,durationMs:number,votes:Map<string,string>}|null} */
let announcement = null;
let countsDirty = false;

function tokenOk(given) {
  if (!ADMIN_TOKEN || typeof given !== "string") return false;
  const a = crypto.createHash("sha256").update(given).digest();
  const b = crypto.createHash("sha256").update(ADMIN_TOKEN).digest();
  return crypto.timingSafeEqual(a, b);
}

function tally() {
  let likes = 0;
  let dislikes = 0;
  if (announcement) for (const v of announcement.votes.values()) v === "like" ? likes++ : dislikes++;
  return { likes, dislikes };
}

function announcementPayload() {
  const t = now();
  return {
    id: announcement.id,
    text: announcement.text,
    username: announcement.username,
    avatar: announcement.avatar,
    verified: true,
    createdAt: announcement.createdAt,
    durationMs: announcement.durationMs,
    remainingMs: Math.max(0, announcement.createdAt + announcement.durationMs - t),
    online: globals.size,
    ...tally(),
  };
}

function broadcastGlobal(type, payload) {
  const data = JSON.stringify({ type, ...payload });
  for (const ws of globals) if (ws.readyState === WebSocket.OPEN) ws.send(data);
}

setInterval(() => {
  if (!countsDirty || !announcement) return;
  countsDirty = false;
  broadcastGlobal("announcement_counts", { id: announcement.id, online: globals.size, ...tally() });
}, COUNTS_PUSH_MS).unref();

// ------------------------------------------------------------- community ---
/** Keeps line breaks (unlike cleanString) for chat text. */
function cleanChat(v, max) {
  if (typeof v !== "string") return "";
  return v.replace(/[\u0000-\u0009\u000b-\u001f\u007f]/g, "").trim().slice(0, max);
}

function lowerName(ws) {
  return ws.chat ? ws.chat.name.toLowerCase() : "";
}

function handleCommunity(ws, type, msg) {
  if (type === "chat_join") {
    const uid = cleanString(msg.uid, 64);
    const name = cleanString(msg.name, NAME_MAX_LEN);
    if (!uid || !name) return true;
    ws.chat = {
      uid,
      name,
      avatar: cleanString(msg.avatar, 400),
      decoration: cleanString(msg.decoration, 40),
      nameFont: cleanString(msg.nameFont, 40),
    };
    ws.chatOpen = false;
    ws.lastChat = 0;
    globals.add(ws);
    ws.global = true;
    return true;
  }
  if (!ws.chat) return type.startsWith("chat_");
  if (type === "chat_open") {
    ws.chatOpen = !!msg.open;
    return true;
  }
  if (type === "chat_typing") {
    const data = JSON.stringify({ type: "chat_typing", uid: ws.chat.uid, name: ws.chat.name });
    for (const o of globals) if (o !== ws && o.chatOpen && o.readyState === WebSocket.OPEN) o.send(data);
    return true;
  }
  if (type === "chat_send") {
    const t = now();
    if (t - ws.lastChat < COMMUNITY_MIN_GAP_MS) return sendError(ws, "rate_limited", "slow down"), true;
    const text = cleanChat(msg.text, COMMUNITY_TEXT_MAX);
    let image = "";
    if (typeof msg.image === "string" && msg.image) {
      if (!msg.image.startsWith("data:image/jpeg;base64,") || msg.image.length > COMMUNITY_IMAGE_MAX) {
        return sendError(ws, "bad_message", "photo too large"), true;
      }
      image = msg.image;
    }
    if (!text && !image) return true;
    ws.lastChat = t;
    const dev = tokenOk(msg.token);
    const everyone = dev && /(^|\s)@everyone\b/i.test(text);
    const wanted = new Set();
    for (const m of text.matchAll(/@([A-Za-z0-9_.]{3,20})/g)) wanted.add(m[1].toLowerCase());
    const mentions = [];
    for (const o of globals) {
      const n = lowerName(o);
      if (n && wanted.has(n) && !mentions.includes(o.chat.name)) mentions.push(o.chat.name);
    }
    const out = {
      id: cleanString(msg.id, 40) || crypto.randomUUID(),
      uid: ws.chat.uid,
      name: ws.chat.name,
      avatar: ws.chat.avatar,
      decoration: ws.chat.decoration,
      nameFont: ws.chat.nameFont,
      text,
      image,
      at: t,
      dev,
      everyone,
      mentions,
    };
    const full = JSON.stringify({ type: "chat_msg", ...out });
    const pop = JSON.stringify({ type: "chat_mention", ...out, image: "", hasImage: !!image });
    for (const o of globals) {
      if (o === ws || o.readyState !== WebSocket.OPEN) continue;
      if (o.chatOpen) o.send(full);
      else if (everyone || (o.chat && mentions.includes(o.chat.name))) o.send(pop);
    }
    send(ws, "chat_ack", { id: out.id, at: t, dev, everyone, mentions });
    return true;
  }
  if (type === "chat_delete") {
    // only the developer removes other people's messages; everyone sees it vanish at once
    if (!tokenOk(msg.token)) return sendError(ws, "forbidden", "wrong admin token"), true;
    const id = cleanString(msg.id, 40);
    if (id) broadcastGlobal("chat_deleted", { id });
    return true;
  }
  return false;
}

function handleGlobal(ws, type, msg) {
  if (type.startsWith("chat_")) return handleCommunity(ws, type, msg);
  if (type === "global_join") {
    globals.add(ws);
    ws.global = true;
    send(ws, "global_ready", { online: globals.size });
    if (announcement && now() - announcement.createdAt < Math.min(ANNOUNCE_LATE_JOIN_MS, announcement.durationMs)) {
      send(ws, "announcement", announcementPayload());
    }
    countsDirty = true;
    return true;
  }
  if (type === "global_leave") {
    globals.delete(ws);
    ws.global = false;
    return true;
  }
  if (type === "announce") {
    if (!ADMIN_TOKEN) return sendError(ws, "announce_disabled", "ADMIN_TOKEN is not set on the server"), true;
    if (!tokenOk(msg.token)) return sendError(ws, "forbidden", "wrong admin token"), true;
    const text = cleanString(msg.text, ANNOUNCE_TEXT_MAX);
    if (!text) return sendError(ws, "bad_message", "empty announcement"), true;
    const avatar = cleanString(msg.avatar, 400);
    announcement = {
      id: crypto.randomUUID(),
      text,
      username: cleanString(msg.username, NAME_MAX_LEN) || "Kiro",
      avatar: /^https?:\/\//.test(avatar) ? avatar : "",
      createdAt: now(),
      durationMs: Math.min(ANNOUNCE_MAX_MS, Math.max(ANNOUNCE_MIN_MS, parseInt(msg.durationMs, 10) || 12_000)),
      votes: new Map(),
    };
    const payload = announcementPayload();
    broadcastGlobal("announcement", payload);
    send(ws, "announce_ok", { id: announcement.id, delivered: globals.size });
    return true;
  }
  if (type === "announce_clear") {
    if (!tokenOk(msg.token)) return sendError(ws, "forbidden", "wrong admin token"), true;
    if (announcement) broadcastGlobal("announcement_end", { id: announcement.id });
    announcement = null;
    return true;
  }
  if (type === "react") {
    const voter = cleanString(msg.voter, 64);
    if (!announcement || !voter || msg.id !== announcement.id) return true;
    if (msg.choice === "like" || msg.choice === "dislike") announcement.votes.set(voter, msg.choice);
    else announcement.votes.delete(voter);
    countsDirty = true;
    return true;
  }
  return false;
}

// ---------------------------------------------------------- msg handlers ---
const HOST_ONLY = new Set(["play", "pause", "seek", "rate", "change_video", "host_settings", "transfer_host", "kick", "browse"]);

function handleMessage(ws, msg) {
  const type = msg && msg.type;
  if (typeof type !== "string") return sendError(ws, "bad_message", "missing type");

  if (type === "ping") return send(ws, "pong", { t: msg.t ?? null, serverTime: now() });

  if (handleGlobal(ws, type, msg)) return;

  if (type === "create_room") {
    if (ws.roomId) return sendError(ws, "already_in_room", "leave current room first");
    const userId = cleanString(msg.userId, 64) || crypto.randomUUID();
    const name = cleanString(msg.name, NAME_MAX_LEN) || "Guest";
    const member = { userId, name, ws: null, joinedAt: now(), disconnectedAt: null };
    const room = createRoom(member);
    attachMember(ws, room, member);
    const video = cleanVideo(msg.video);
    if (video) room.video = video;
    return send(ws, "room_created", joinedPayload(room, member));
  }

  if (type === "join_room") {
    if (ws.roomId) return sendError(ws, "already_in_room", "leave current room first");
    const roomId = cleanString(msg.roomId, 12).toUpperCase();
    const room = rooms.get(roomId);
    if (!room) return sendError(ws, "room_not_found", "Room not found");
    const userId = cleanString(msg.userId, 64) || crypto.randomUUID();
    const name = cleanString(msg.name, NAME_MAX_LEN) || "Guest";

    let member = room.members.get(userId);
    if (member) {
      // reconnect: take over the old (possibly stale) socket
      const old = member.ws;
      member.name = name;
      if (old && old !== ws) {
        old.roomId = null;
        send(old, "replaced", {});
        old.close(4000, "replaced by new connection");
      }
      attachMember(ws, room, member);
      send(ws, "joined", joinedPayload(room, member));
      return broadcast(room, "member_online", { userId, name, members: memberList(room) }, userId);
    }

    if (room.members.size >= MAX_ROOM_SIZE) return sendError(ws, "room_full", "Room is full");
    member = { userId, name, ws: null, joinedAt: now(), disconnectedAt: null };
    room.members.set(userId, member);
    attachMember(ws, room, member);
    send(ws, "joined", joinedPayload(room, member));
    return broadcast(room, "member_joined", { userId, name, members: memberList(room) }, userId);
  }

  // everything below needs a room
  const room = ws.roomId ? rooms.get(ws.roomId) : null;
  if (!room) return sendError(ws, "not_in_room", "join a room first");
  const member = room.members.get(ws.userId);
  if (!member) return sendError(ws, "not_in_room", "join a room first");
  const isHost = room.hostId === member.userId;

  if (HOST_ONLY.has(type) && !isHost) {
    return sendError(ws, "not_host", "Only the host can do that");
  }

  const p = room.playback;

  switch (type) {
    case "leave_room":
      removeMember(room, member.userId);
      return send(ws, "left", {});

    case "sync_request":
      return send(ws, "state", playbackSnapshot(room));

    case "play": {
      p.position = cleanNumber(msg.time, 0, 1e7, currentPosition(room));
      p.playing = true;
      p.updatedAt = now();
      return broadcast(room, "state", { ...playbackSnapshot(room, member.userId), action: "play" });
    }
    case "pause": {
      p.position = cleanNumber(msg.time, 0, 1e7, currentPosition(room));
      p.playing = false;
      p.updatedAt = now();
      return broadcast(room, "state", { ...playbackSnapshot(room, member.userId), action: "pause" });
    }
    case "seek": {
      p.position = cleanNumber(msg.time, 0, 1e7, currentPosition(room));
      p.updatedAt = now();
      return broadcast(room, "state", { ...playbackSnapshot(room, member.userId), action: "seek" });
    }
    case "rate": {
      p.position = currentPosition(room);
      p.rate = cleanNumber(msg.rate, 0.25, 4, 1);
      p.updatedAt = now();
      return broadcast(room, "state", { ...playbackSnapshot(room, member.userId), action: "rate" });
    }
    case "change_video": {
      const video = cleanVideo(msg.video);
      if (!video) return sendError(ws, "bad_video", "invalid video");
      room.video = video;
      p.position = cleanNumber(msg.time, 0, 1e7, 0);
      p.playing = msg.autoplay === true;
      p.updatedAt = now();
      return broadcast(room, "state", {
        ...playbackSnapshot(room, member.userId),
        action: "change_video",
      });
    }
    case "browse": {
      room.browse = cleanBrowse(msg.browse);
      return broadcast(room, "browse", { browse: room.browse }, member.userId);
    }
    case "host_settings": {
      room.settings = cleanSettings(msg.settings);
      return broadcast(
        room,
        "state",
        { ...playbackSnapshot(room, member.userId), action: "settings" },
        member.userId
      );
    }
    case "host_heartbeat": {
      // optional: host reports its real position so drift never builds up
      if (!isHost) return;
      p.position = cleanNumber(msg.time, 0, 1e7, p.position);
      p.playing = msg.playing === true;
      p.updatedAt = now();
      return broadcast(
        room,
        "state",
        { ...playbackSnapshot(room, member.userId), action: "heartbeat" },
        member.userId
      );
    }
    case "buffering": {
      // anyone can tell the room they're buffering (host app may choose to wait)
      return broadcast(
        room,
        "buffering",
        { userId: member.userId, name: member.name, buffering: msg.buffering === true },
        member.userId
      );
    }
    case "chat": {
      const text = cleanString(msg.text, CHAT_MAX_LEN);
      if (!text) return;
      return broadcast(room, "chat", {
        userId: member.userId,
        name: member.name,
        text,
        ts: now(),
      });
    }
    // ---- voice call (audio itself goes phone to phone; the server only relays the set-up notes and who is in the call)
    case "voice_join": {
      member.voice = { muted: msg.muted === true, deafened: msg.deafened === true };
      return broadcastVoice(room);
    }
    case "voice_leave": {
      return leaveVoice(room, member);
    }
    case "voice_mute": {
      if (!member.voice) return;
      member.voice.muted = msg.muted === true;
      member.voice.deafened = msg.deafened === true;
      return broadcastVoice(room);
    }
    case "voice_signal": {
      if (!member.voice) return;
      const target = room.members.get(cleanString(msg.to, 64));
      if (!target || !target.voice || !target.ws) return;
      const data = msg.data && typeof msg.data === "object" && !Array.isArray(msg.data) ? msg.data : null;
      if (!data) return;
      let text = "";
      try {
        text = JSON.stringify(data);
      } catch (_) {}
      if (!text || text.length > VOICE_SIGNAL_MAX) return;
      return send(target.ws, "voice_signal", { from: member.userId, data });
    }
    case "chat": {
      // Live chat fan-out: the app also saves the message in Firestore; this only makes it arrive at once.
      const text = cleanString(msg.text, 500);
      const id = cleanString(msg.id, 40);
      if (!text || !id) return;
      return broadcast(
        room,
        "chat",
        {
          from: member.userId,
          id,
          name: cleanString(msg.name, 40),
          avatar: cleanString(msg.avatar, 400),
          decoration: cleanString(msg.decoration, 40),
          text,
          at: cleanNumber(msg.at, 0, 9e15, now()),
          edited: msg.edited === true,
        },
        member.userId
      );
    }
    case "chat_delete": {
      const id = cleanString(msg.id, 40);
      if (!id) return;
      return broadcast(room, "chat_delete", { from: member.userId, id }, member.userId);
    }
    case "speaking": {
      if (!member.voice) return;
      return broadcast(room, "speaking", { userId: member.userId, on: msg.on === true }, member.userId);
    }
    case "transfer_host": {
      const target = room.members.get(cleanString(msg.userId, 64));
      if (!target || !target.ws) return sendError(ws, "bad_target", "member not found or offline");
      room.hostId = target.userId;
      return broadcast(room, "host_changed", { hostId: room.hostId, members: memberList(room) });
    }
    case "kick": {
      const targetId = cleanString(msg.userId, 64);
      if (targetId === member.userId) return sendError(ws, "bad_target", "cannot kick yourself");
      const target = room.members.get(targetId);
      if (!target) return sendError(ws, "bad_target", "member not found");
      const targetWs = target.ws;
      removeMember(room, targetId, { reason: "kicked" });
      if (targetWs) targetWs.close(4001, "kicked");
      return;
    }
    default:
      return sendError(ws, "unknown_type", `unknown message type: ${type}`);
  }
}

// ----------------------------------------------------------- http server ---
function json(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Content-Length": Buffer.byteLength(data),
    "Access-Control-Allow-Origin": "*",
    "Cache-Control": "no-store",
  });
  res.end(data);
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://localhost");

  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    });
    return res.end();
  }

  if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/health")) {
    return json(res, 200, {
      ok: true,
      service: "kiro-watchparty",
      rooms: rooms.size,
      uptime: Math.round(process.uptime()),
    });
  }

  // lets the app check a room code before opening the socket
  const m = url.pathname.match(/^\/rooms\/([A-Za-z0-9]{1,12})$/);
  if (req.method === "GET" && m) {
    const room = rooms.get(m[1].toUpperCase());
    if (!room) return json(res, 404, { exists: false });
    return json(res, 200, {
      exists: true,
      roomId: room.id,
      members: room.members.size,
      full: room.members.size >= MAX_ROOM_SIZE,
      video: room.video ? { title: room.video.title, episode: room.video.episode } : null,
    });
  }

  json(res, 404, { error: "not_found" });
});

// ------------------------------------------------------------- websocket ---
const wss = new WebSocketServer({
  server,
  path: "/ws",
  maxPayload: MAX_MESSAGE_BYTES,
  verifyClient: (info, done) => {
    if (ALLOWED_ORIGINS.length) {
      const origin = info.origin;
      if (origin && !ALLOWED_ORIGINS.includes(origin)) return done(false, 403, "origin not allowed");
    }
    if (WS_TOKEN) {
      const token = new URL(info.req.url, "http://localhost").searchParams.get("token");
      if (token !== WS_TOKEN) return done(false, 401, "unauthorized");
    }
    done(true);
  },
});

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.roomId = null;
  ws.userId = null;
  ws.tokens = RATE_LIMIT_PER_SEC;
  ws.lastRefill = now();

  send(ws, "connected", { serverTime: now() });

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (raw, isBinary) => {
    if (isBinary) return sendError(ws, "bad_message", "binary not supported");

    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (_) {
      return sendError(ws, "bad_json", "invalid JSON");
    }

    // simple token bucket rate limit (voice set-up notes come in bursts, so they cost less)
    const t = now();
    ws.tokens = Math.min(RATE_LIMIT_PER_SEC, ws.tokens + ((t - ws.lastRefill) / 1000) * RATE_LIMIT_PER_SEC);
    ws.lastRefill = t;
    const type = msg && msg.type;
    if (raw.length > SMALL_MESSAGE_BYTES && type !== "chat_send") return sendError(ws, "bad_message", "message too large");
    const cost = type === "voice_signal" ? 0.2 : type === "speaking" ? 0.5 : 1;
    if (ws.tokens < cost) return sendError(ws, "rate_limited", "slow down");
    ws.tokens -= cost;
    try {
      handleMessage(ws, msg);
    } catch (err) {
      console.error("handler error:", err);
      sendError(ws, "server_error", "something went wrong");
    }
  });

  ws.on("close", () => {
    globals.delete(ws);
    handleDisconnect(ws);
  });
  ws.on("error", (err) => console.error("socket error:", err.message));
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, HEARTBEAT_MS);
wss.on("close", () => clearInterval(heartbeat));

// -------------------------------------------------------------- lifecycle ---
if (require.main === module) {
  server.listen(PORT, "0.0.0.0", () => console.log(`watchparty server listening on :${PORT} (ws path /ws)`));

  const shutdown = () => {
    console.log("shutting down...");
    for (const ws of wss.clients) ws.close(1001, "server restarting");
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 5000).unref();
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

module.exports = { server, wss, rooms, globals };
