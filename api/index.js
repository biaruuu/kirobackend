// Vercel serverless functions cannot hold WebSocket connections open.
// This endpoint only exists so a Vercel deploy succeeds and reports clearly.
// Run the real WebSocket server (server.js) on Render, Railway or Fly.
module.exports = (req, res) => {
  res.setHeader("Content-Type", "application/json");
  res.status(200).json({
    ok: true,
    websocket: false,
    note: "WebSockets are not supported on Vercel. Deploy server.js on Render/Railway/Fly and point the app to wss://<host>/ws",
  });
};
