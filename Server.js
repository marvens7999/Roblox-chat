const express = require("express");
const http = require("http");
const crypto = require("crypto");
const { WebSocketServer, WebSocket } = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT) || 10000;

//========================================================
// CONFIG
//========================================================

const ADMIN_USER_ID = "10909271675";

const MAX_ROOM_ID_LENGTH = 200;
const MAX_PLAYER_ID_LENGTH = 100;
const MAX_DISPLAY_NAME_LENGTH = 100;
const MAX_CHAT_LENGTH = 300;
const MAX_COMMAND_LENGTH = 300;

//========================================================
// WEBSOCKET SERVER
//========================================================

const wss = new WebSocketServer({
    server,
    path: "/chat"
});

//========================================================
// ROOMS
//========================================================

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

//========================================================
// SOCKET HELPERS
//========================================================

function send(ws, payload) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        return;
    }

    try {
        ws.send(JSON.stringify(payload));
    } catch {}
}

function broadcast(roomId, payload) {
    const room = rooms.get(roomId);

    if (!room) {
        return;
    }

    const encoded = JSON.stringify(payload);

    for (const client of room.clients) {
        if (client.readyState === WebSocket.OPEN) {
            try {
                client.send(encoded);
            } catch {}
        }
    }
}

function broadcastPresence(roomId) {
    const room = rooms.get(roomId);

    if (!room) {
        return;
    }

    broadcast(roomId, {
        type: "presence",
        online: room.clients.size
    });
}

//========================================================
// ROOM CLEANUP
//========================================================

function removeClientFromRoom(ws) {
    if (!ws.roomId) {
        return;
    }

    const roomId = ws.roomId;
    const room = rooms.get(roomId);

    ws.roomId = null;

    if (!room) {
        return;
    }

    room.clients.delete(ws);

    if (room.clients.size === 0) {
        rooms.delete(roomId);
        return;
    }

    broadcastPresence(roomId);
}

//========================================================
// HTTP HEALTH CHECK
//========================================================

app.get("/", (req, res) => {
    res.json({
        status: "online",
        service: "ZERO CHAT Relay",
        rooms: rooms.size,
        connections: wss.clients.size
    });
});

//========================================================
// WEBSOCKET CONNECTION
//========================================================

wss.on("connection", (ws) => {

    ws.id = crypto.randomUUID();

    ws.roomId = null;
    ws.playerId = null;
    ws.displayName = null;

    ws.isAlive = true;

    //====================================================
    // HEARTBEAT
    //====================================================

    ws.on("pong", () => {
        ws.isAlive = true;
    });

    //====================================================
    // MESSAGE
    //====================================================

    ws.on("message", (raw) => {

        let data;

        try {
            data = JSON.parse(raw.toString());
        } catch {
            return;
        }

        if (!data || typeof data !== "object") {
            return;
        }

        //================================================
        // JOIN ROOM
        //================================================

        if (data.type === "join") {

            const roomId = String(
                data.roomId || ""
            ).trim();

            if (
                !roomId ||
                roomId.length > MAX_ROOM_ID_LENGTH
            ) {
                send(ws, {
                    type: "error",
                    message: "Invalid room ID."
                });

                return;
            }

            // Remove previous room membership.
            removeClientFromRoom(ws);

            ws.roomId = roomId;

            ws.playerId = String(
                data.playerId || ""
            ).slice(0, MAX_PLAYER_ID_LENGTH);

            ws.displayName = String(
                data.displayName || "Player"
            ).slice(0, MAX_DISPLAY_NAME_LENGTH);

            const room = getRoom(roomId);

            room.clients.add(ws);

            send(ws, {
                type: "joined",
                roomId: roomId,
                online: room.clients.size
            });

            broadcastPresence(roomId);

            return;
        }

        //================================================
        // CHAT
        //================================================

        if (data.type === "chat") {

            if (!ws.roomId) {
                return;
            }

            const text = String(
                data.text || ""
            ).trim();

            if (!text) {
                return;
            }

            const safeText = text.slice(
                0,
                MAX_CHAT_LENGTH
            );

            const room = rooms.get(ws.roomId);

            if (!room) {
                return;
            }

            broadcast(ws.roomId, {
                type: "chat",
                playerId: ws.playerId,
                displayName: ws.displayName,
                text: safeText,
                timestamp: Date.now()
            });

            return;
        }

        //================================================
        // ADMIN COMMAND
        //================================================

        if (data.type === "admin_command") {

            if (!ws.roomId) {
                return;
            }

            // Server-side admin verification.
            if (
                String(ws.playerId) !==
                ADMIN_USER_ID
            ) {
                send(ws, {
                    type: "error",
                    message: "Unauthorized."
                });

                return;
            }

            const commandString = String(
                data.commandString || ""
            ).trim();

            if (!commandString) {
                return;
            }

            // Only accept commands.
            if (commandString.charAt(0) !== ";") {
                return;
            }

            const safeCommand = commandString.slice(
                0,
                MAX_COMMAND_LENGTH
            );

            const room = rooms.get(ws.roomId);

            if (!room) {
                return;
            }

            // Relay the command to every ZERO CHAT client.
            broadcast(ws.roomId, {
                type: "admin_sync",
                commandString: safeCommand,
                adminUserId: ADMIN_USER_ID,
                timestamp: Date.now()
            });

            return;
        }

        //================================================
        // PING
        //================================================

        if (data.type === "ping") {

            send(ws, {
                type: "pong",
                timestamp: Date.now()
            });

            return;
        }
    });

    //====================================================
    // CLOSE
    //====================================================

    ws.on("close", () => {
        removeClientFromRoom(ws);
    });

    //====================================================
    // ERROR
    //====================================================

    ws.on("error", () => {
        removeClientFromRoom(ws);
    });
});

//========================================================
// SERVER HEARTBEAT
//========================================================

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

//========================================================
// CLEAN SHUTDOWN
//========================================================

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

//========================================================
// START
//========================================================

server.listen(
    PORT,
    "0.0.0.0",
    () => {
        console.log(
            `ZERO CHAT relay listening on port ${PORT}`
        );
    }
);
