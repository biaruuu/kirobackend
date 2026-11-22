"use strict";
// End-to-end smoke test. Needs Node 22+ (global WebSocket client).
const assert = require("assert");
const { server, wss } = require("../server");

const PORT = 18080;

function client() {
  const ws = new WebSocket(`ws://127.0.0.1:${PORT}/ws`);
  const inbox = [];
  const waiters = [];
  ws.onmessage = (e) => {
    const m = JSON.parse(e.data);
    const i = waiters.findIndex((w) => w.type === m.type);
    if (i >= 0) waiters.splice(i, 1)[0].resolve(m);
    else inbox.push(m);
  };
  const opened = new Promise((r) => (ws.onopen = r));
  return {
    ws,
    opened,
    send: (o) => ws.send(JSON.stringify(o)),
    next: (type, ms = 2000) =>
      new Promise((resolve, reject) => {
        const i = inbox.findIndex((m) => m.type === type);
        if (i >= 0) return resolve(inbox.splice(i, 1)[0]);
        const t = setTimeout(() => reject(new Error(`timeout waiting for ${type}`)), ms);
        waiters.push({ type, resolve: (m) => (clearTimeout(t), resolve(m)) });
      }),
  };
}

(async () => {
  await new Promise((r) => server.listen(PORT, r));

  const host = client();
  const guest = client();
  await Promise.all([host.opened, guest.opened]);

  host.send({ type: "create_room", name: "Host", userId: "h1", video: { url: "https://x.test/a.m3u8", title: "Ep 1" } });
  const created = await host.next("room_created");
  assert.strictEqual(created.roomId.length, 6);
  assert.strictEqual(created.you.isHost, true);
  const roomId = created.roomId;

  guest.send({ type: "join_room", roomId, name: "Guest", userId: "g1" });
  const joined = await guest.next("joined");
  assert.strictEqual(joined.you.isHost, false);
  assert.strictEqual(joined.state.video.title, "Ep 1");
  assert.strictEqual((await host.next("member_joined")).name, "Guest");

  // host play -> guest receives
  host.send({ type: "play", time: 12.5 });
  const play = await guest.next("state");
  assert.strictEqual(play.action, "play");
  assert.strictEqual(play.playing, true);
  assert.ok(play.position >= 12.5);

  // host seek + pause
  host.send({ type: "seek", time: 100 });
  assert.strictEqual((await guest.next("state")).action, "seek");
  host.send({ type: "pause", time: 101 });
  const paused = await guest.next("state");
  assert.strictEqual(paused.playing, false);
  assert.strictEqual(paused.position, 101);

  // host settings (source, subtitle, speed, fullscreen) reach guests and late joiners
  host.send({ type: "host_settings", settings: { provider: "Anivault", audio: "English", sub: "en", fullscreen: true, speed: 1.5, subFont: "Satoshi", subSize: 1, subBg: 0, subEdge: 2 } });
  const st = await guest.next("state");
  assert.strictEqual(st.action, "settings");
  assert.strictEqual(st.settings.provider, "Anivault");
  assert.strictEqual(st.settings.fullscreen, true);
  assert.strictEqual(st.settings.speed, 1.5);
  assert.strictEqual(st.settings.subFont, "Satoshi");
  assert.strictEqual(st.settings.subEdge, 2);
  guest.send({ type: "host_settings", settings: { provider: "Hacked" } });
  assert.strictEqual((await guest.next("error")).code, "not_host");

  // guest cannot control playback
  guest.send({ type: "play", time: 0 });
  assert.strictEqual((await guest.next("error")).code, "not_host");

  // chat reaches everyone
  guest.send({ type: "chat", text: "hello" });
  assert.strictEqual((await host.next("chat")).text, "hello");

  // late joiner gets current state
  const late = client();
  await late.opened;
  late.send({ type: "join_room", roomId, name: "Late", userId: "l1" });
  const lj = await late.next("joined");
  assert.strictEqual(lj.state.position, 101);
  assert.strictEqual(lj.state.settings.provider, "Anivault");

  // voice: join, state for everyone, relay of set-up notes, mute, speaking, leave
  guest.send({ type: "voice_join", muted: false });
  late.send({ type: "voice_join", muted: true });
  let vs = await host.next("voice_state");
  while (vs.members.length < 2) vs = await host.next("voice_state");
  assert.deepStrictEqual(vs.members.map((m) => m.userId).sort(), ["g1", "l1"]);
  guest.send({ type: "voice_signal", to: "l1", data: { type: "offer", sdp: "v=0" } });
  const sig = await late.next("voice_signal");
  assert.strictEqual(sig.from, "g1");
  assert.strictEqual(sig.data.type, "offer");
  host.send({ type: "voice_signal", to: "l1", data: { type: "offer", sdp: "x" } }); // host is not in the call: dropped
  guest.send({ type: "speaking", on: true });
  const sp = await late.next("speaking");
  assert.strictEqual(sp.userId, "g1");
  assert.strictEqual(sp.on, true);
  late.send({ type: "voice_mute", muted: false, deafened: true });
  let vs2 = await guest.next("voice_state");
  while (!vs2.members.find((m) => m.userId === "l1" && m.deafened)) vs2 = await guest.next("voice_state");
  late.send({ type: "voice_leave" });
  let vs3 = await guest.next("voice_state");
  while (vs3.members.length !== 1) vs3 = await guest.next("voice_state");
  assert.strictEqual(vs3.members[0].userId, "g1");

  // bad room
  const bad = client();
  await bad.opened;
  bad.send({ type: "join_room", roomId: "ZZZZZZ", name: "X" });
  assert.strictEqual((await bad.next("error")).code, "room_not_found");

  // http endpoints
  const health = await (await fetch(`http://127.0.0.1:${PORT}/health`)).json();
  assert.strictEqual(health.ok, true);
  const info = await (await fetch(`http://127.0.0.1:${PORT}/rooms/${roomId}`)).json();
  assert.strictEqual(info.members, 3);

  // host leaves -> host migrates to oldest remaining member
  host.send({ type: "leave_room" });
  const hc = await guest.next("host_changed");
  assert.strictEqual(hc.hostId, "g1");

  console.log("smoke test passed");
  for (const c of [host, guest, late, bad]) c.ws.close();
  wss.close();
  server.close();
  setTimeout(() => process.exit(0), 200);
})().catch((e) => {
  console.error("smoke test FAILED:", e);
  process.exit(1);
});
