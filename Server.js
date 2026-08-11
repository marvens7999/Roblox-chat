const express = require("express");
const http = require("http");
const crypto = require("crypto");
const { WebSocketServer, WebSocket } = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT) || 10000;

const wss = new WebSocketServer({
    server,
    path: "/chat"
});

/*
    rooms:
    roomId -> {
        clients: Set<WebSocket>,
        createdAt: number
    }
*/

const rooms = new Map();

function createRoom(roomId) {
    const room = {
        clients: new Set(),
        createdAt: Date.now()
    };

    rooms.set(roomId, room);
    return room;
}

function getRoom(roomId) {
    return rooms.get(roomId) || createRoom(roomId);
}

function removeClientFromRoom(ws) {
    if (!ws.roomId) return;

    const room = rooms.get(ws.roomId);

    if (!room) {
        ws.roomId = null;
        return;
    }

    room.clients.delete(ws);

    const roomId = ws.roomId;
    ws.roomId = null;

    // Automatically delete empty rooms.
    if (room.clients.size === 0) {
        rooms.delete(roomId);
        return;
    }

    broadcastPresence(roomId);
}

function send(ws, payload) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(payload));
    }
}

function broadcast(roomId, payload) {
    const room = rooms.get(roomId);

    if (!room) return;

    const data = JSON.stringify(payload);

    for (const client of room.clients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(data);
        }
    }
}

function broadcastPresence(roomId) {
    const room = rooms.get(roomId);

    if (!room) return;

    broadcast(roomId, {
        type: "presence",
        online: room.clients.size
    });
}

/*
    Basic HTTP health endpoint.
*/

app.get("/", (req, res) => {
    res.json({
        status: "online",
        service: "Roblox Chat Relay",
        rooms: rooms.size,
        connections: wss.clients.size
    });
});

/*
    WebSocket connection.
*/

wss.on("connection", (ws) => {
    ws.id = crypto.randomUUID();

    ws.roomId = null;
    ws.playerId = null;
    ws.displayName = null;

    ws.isAlive = true;

    ws.on("pong", () => {
        ws.isAlive = true;
    });

    ws.on("message", (raw) => {
        let data;

        try {
            data = JSON.parse(raw.toString());
        } catch {
            return;
        }

        /*
            JOIN ROOM
        */

        if (data.type === "join") {
            const roomId = String(data.roomId || "").trim();

            if (!roomId || roomId.length > 200) {
                send(ws, {
                    type: "error",
                    message: "Invalid room ID."
                });

                return;
            }

            // Remove old room membership first.
            removeClientFromRoom(ws);

            ws.roomId = roomId;
            ws.playerId = String(data.playerId || "").slice(0, 100);
            ws.displayName = String(
                data.displayName || "Player"
            ).slice(0, 100);

            const room = getRoom(roomId);

            room.clients.add(ws);

            send(ws, {
                type: "joined",
                roomId,
                online: room.clients.size
            });

            broadcastPresence(roomId);

            return;
        }

        /*
            CHAT MESSAGE
        */

        if (data.type === "chat") {
            if (!ws.roomId) return;

            const text = String(data.text || "").trim();

            if (!text) return;

            // Prevent huge messages.
            const safeText = text.slice(0, 300);

            const room = rooms.get(ws.roomId);

            if (!room) return;

            broadcast(ws.roomId, {
                type: "chat",
                playerId: ws.playerId,
                displayName: ws.displayName,
                text: safeText,
                timestamp: Date.now()
            });

            return;
        }

        /*
            OPTIONAL PING FROM CLIENT
        */

        if (data.type === "ping") {
            send(ws, {
                type: "pong",
                timestamp: Date.now()
            });

            return;
        }
    });

    ws.on("close", () => {
        removeClientFromRoom(ws);
    });

    ws.on("error", () => {
        removeClientFromRoom(ws);
    });
});

/*
    Server-side heartbeat.

    This prevents disconnected clients from remaining
    counted as online.
*/

const heartbeatInterval = setInterval(() => {
    for (const ws of wss.clients) {
        if (ws.isAlive === false) {
            ws.terminate();
            continue;
        }

        ws.isAlive = false;

        try {
            ws.ping();
        } catch {
            ws.terminate();
        }
    }
}, 30000);

/*
    Clean shutdown.
*/

function shutdown() {
    clearInterval(heartbeatInterval);

    for (const ws of wss.clients) {
        try {
            ws.close();
        } catch {}
    }

    server.close(() => {
        process.exit(0);
    });
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

server.listen(PORT, "0.0.0.0", () => {
    console.log(`Chat relay listening on port ${PORT}`);
});
