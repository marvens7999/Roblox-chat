import express from "express";
import http from "http";
import { WebSocketServer } from "ws";

const app = express();
const server = http.createServer(app);

const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 10000;

// Temporary:
// serverId -> connected WebSocket clients
const gameServers = new Map();

app.get("/", (req, res) => {
  res.json({
    status: "online",
    service: "roblox-chat"
  });
});

wss.on("connection", (socket) => {
  socket.serverId = null;

  socket.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // Player joins a specific Roblox game server
    if (data.type === "JOIN_SERVER") {
      const serverId = data.serverId;

      if (typeof serverId !== "string" || !serverId) {
        return;
      }

      socket.serverId = serverId;

      if (!gameServers.has(serverId)) {
        gameServers.set(serverId, new Set());
      }

      gameServers.get(serverId).add(socket);

      socket.send(JSON.stringify({
        type: "JOINED_SERVER",
        serverId
      }));

      return;
    }

    // Player sends a chat message
    if (data.type === "CHAT_MESSAGE") {
      if (!socket.serverId) {
        return;
      }

      if (
        typeof data.username !== "string" ||
        typeof data.message !== "string"
      ) {
        return;
      }

      const message = {
        type: "CHAT_MESSAGE",
        username: data.username,
        message: data.message
      };

      // ONLY broadcast to players in THIS game server.
      const players = gameServers.get(socket.serverId);

      if (!players) {
        return;
      }

      for (const player of players) {
        if (player.readyState === 1) {
          player.send(JSON.stringify(message));
        }
      }
    }
  });

  socket.on("close", () => {
    if (!socket.serverId) {
      return;
    }

    const players = gameServers.get(socket.serverId);

    if (!players) {
      return;
    }

    players.delete(socket);

    if (players.size === 0) {
      gameServers.delete(socket.serverId);
    }
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Chat server running on port ${PORT}`);
});
