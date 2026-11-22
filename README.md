# kirobackend - Watch Party WebSocket Server

Realtime server para sa watch party ng KiroApp. Host ang may control (play, pause, seek, speed, change video), at lahat ng members ay nasi-sync agad.

## Deploy

**Render (recommended, gumagana ang WebSocket):**
1. Render > New > Blueprint (o Web Service) > piliin ang repo na ito.
2. Build: `npm install`, Start: `npm start` (nasa `render.yaml` na).
3. WebSocket URL: `wss://<your-service>.onrender.com/ws`

**Vercel:** hindi suportado ang WebSocket sa Vercel (serverless). Ang `api/index.js` ay status endpoint lang. Para sa watch party, gamitin ang Render / Railway / Fly.

Free Render plan natutulog kapag walang traffic, kaya may delay sa unang connect. Gamitin ang `GET /health` para i-wake up.

## Env (optional)

| Name | Gamit |
|---|---|
| `PORT` | default 8080 (Render ang nagse-set) |
| `WS_TOKEN` | kung nakalagay, connect gamit `/ws?token=...` |
| `ALLOWED_ORIGINS` | comma-separated web origins; native app walang Origin kaya pasok pa rin |
| `MAX_ROOM_SIZE` | default 50 |

## HTTP

- `GET /health` - status
- `GET /rooms/:code` - check kung may room (`exists`, `members`, `full`)

## WebSocket protocol (`/ws`, JSON)

**Client > Server**

| type | fields | sino |
|---|---|---|
| `create_room` | `name`, `userId?`, `video?{url,title,episode}` | lahat |
| `join_room` | `roomId`, `name`, `userId?` | lahat |
| `leave_room` | | lahat |
| `sync_request` | | lahat |
| `chat` | `text` | lahat |
| `buffering` | `buffering: bool` | lahat |
| `ping` | `t?` | lahat |
| `play` / `pause` / `seek` | `time` (seconds) | host |
| `rate` | `rate` (0.25 - 4) | host |
| `change_video` | `video{url,title,episode}`, `time?`, `autoplay?` | host |
| `host_settings` | `settings{provider,audio,sub,fullscreen,speed,subFont,subSize,subBg,subEdge}` (kini-copy ng guests) | host |
| `host_heartbeat` | `time`, `playing` (ipadala every ~5s para walang drift) | host |
| `transfer_host` / `kick` | `userId` | host |
| `voice_join` | `muted?`, `deafened?` | lahat |
| `voice_leave` | | lahat |
| `voice_mute` | `muted`, `deafened` | nasa voice |
| `voice_signal` | `to`, `data{type: offer/answer/ice, ...}` (relay lang; ang audio ay phone-to-phone WebRTC) | nasa voice |
| `speaking` | `on: bool` | nasa voice |

**Server > Client**: `connected`, `room_created`, `joined`, `state`, `member_joined`, `member_left`, `member_online`, `member_offline`, `host_changed`, `chat`, `buffering`, `kicked`, `replaced`, `left`, `pong`, `voice_state{members:[{userId,muted,deafened}]}`, `voice_signal{from,data}`, `speaking{userId,on}`, `error{code,message}`

`state` = `{ playing, position, rate, video, settings, action, by, serverTime }`. Ang `position` ay computed na ng server sa oras na ipinadala. Para sa latency, i-apply ng client: `position + (Date.now() - serverTime)/1000 * rate` kung `playing` (gamitin ang offset mula sa `connected.serverTime` / `pong.serverTime` kung hindi sync ang clock).

**Reconnect:** ulitin ang `join_room` gamit ang parehong `userId` at `roomId`. Kung host ka, may 15 segundo bago ilipat ang host sa iba. Naglalaho ang empty room pagkalipas ng 60 segundo.

**Voice:** ang `joined`/`room_created` ay may `voice` (listahan ng nasa call). Ang `voice_state` ay ipinapadala sa lahat tuwing may nagbago. Kapag nadiskonekta ang socket, 10 segundo munang itinatago ang slot niya sa call (para hindi maputol ang call sa saglit na network blip).

## Local

```
npm install
npm start     # ws://localhost:8080/ws
npm test      # smoke test, Node 22+
```

State ay nasa memory, kaya isang instance lang (huwag mag-scale sa maraming instance nang walang Redis).

### Browse mirror and idle rooms
- `browse` (host only): `{browse: {screen, animeId, title, subtitle, poster, banner, query}}` is stored on the room, sent to everyone else as `browse`, and included in `joined` / `room_created`. Guests use it to show what the host is looking at.
- A room is ended when every member has been offline for 10 minutes.
