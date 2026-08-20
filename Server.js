const express = require("express");
const http = require("http");
const crypto = require("crypto");
const { WebSocketServer, WebSocket } = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT) || 10000;
const ADMIN_USER_ID = "10909271675";

const rooms = new Map();

function createRoom(roomId) {
    const room = {
        clients: new Set(),
        createdAt: Date.now(),
        staffTags: new Map()
    };
    rooms.set(roomId, room);
    return room;
}

function getRoom(roomId) {
    return rooms.get(roomId) || createRoom(roomId);
}

function send(ws, payload) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    try {
        ws.send(JSON.stringify(payload));
        return true;
    } catch {
        return false;
    }
}

function broadcast(roomId, payload) {
    const room = rooms.get(roomId);
    if (!room) return;
    const encoded = JSON.stringify(payload);
    for (const client of room.clients) {
        if (client.readyState === WebSocket.OPEN) {
            try { client.send(encoded); } catch {}
        }
    }
}

function broadcastPresence(roomId) {
    const room = rooms.get(roomId);
    if (!room) return;
    broadcast(roomId, { type: "presence", online: room.clients.size });
}

function getStaffTag(room, playerId) {
    if (!room || !playerId) return "";
    const entry = room.staffTags.get(String(playerId));
    return entry ? String(entry.tag || "") : "";
}

function getStaffTagList(room) {
    if (!room) return [];
    const result = [];
    for (const [playerId, entry] of room.staffTags) {
        result.push({
            playerId: String(playerId),
            displayName: String(entry.displayName || "Player"),
            tag: String(entry.tag || "")
        });
    }
    return result;
}

function findPlayerInRoom(room, name) {
    if (!room) return null;
    const search = String(name || "").trim().toLowerCase();
    if (!search) return null;

    for (const client of room.clients) {
        if (String(client.username || "").toLowerCase() === search || String(client.displayName || "").toLowerCase() === search) {
            return client;
        }
    }
    for (const client of room.clients) {
        if (String(client.playerId || "").toLowerCase() === search) return client;
    }
    for (const client of room.clients) {
        if (String(client.username || "").toLowerCase().startsWith(search)) return client;
    }
    for (const client of room.clients) {
        if (String(client.displayName || "").toLowerCase().startsWith(search)) return client;
    }
    return null;
}

function removeClientFromRoom(ws) {
    const roomId = ws.roomId;
    if (!roomId) return;
    const room = rooms.get(roomId);
    ws.roomId = null;
    if (!room) return;

    room.clients.delete(ws);
    if (room.clients.size === 0) {
        rooms.delete(roomId);
        return;
    }
    broadcastPresence(roomId);
}

const wss = new WebSocketServer({ server, path: "/chat" });

wss.on("connection", (ws) => {
    ws.id = crypto.randomUUID();
    ws.roomId = null;
    ws.playerId = null;
    ws.username = null;
    ws.displayName = null;
    ws.isAlive = true;

    ws.on("pong", () => { ws.isAlive = true; });

    ws.on("message", (raw) => {
        let data;
        try { data = JSON.parse(raw.toString()); } catch { return; }
        if (!data || typeof data !== "object") return;

        if (data.type === "join") {
            const roomId = String(data.roomId || "").trim();
            if (!roomId) return;

            removeClientFromRoom(ws);
            ws.roomId = roomId;
            ws.playerId = String(data.playerId || "");
            ws.displayName = String(data.displayName || "Player");

            const room = getRoom(roomId);
            room.clients.add(ws);

            send(ws, {
                type: "joined",
                roomId,
                online: room.clients.size,
                staffTags: getStaffTagList(room)
            });
            broadcastPresence(roomId);
            return;
        }

        if (!ws.roomId) return;
        const room = rooms.get(ws.roomId);
        if (!room) return;

        if (data.type === "chat") {
            const text = String(data.text || "").trim();
            if (!text) return;

            const safeText = text.slice(0, 300);
            const staffTag = getStaffTag(room, ws.playerId);

            broadcast(ws.roomId, {
                type: "chat",
                playerId: String(ws.playerId || ""),
                displayName: String(ws.displayName || "Player"),
                staffTag,
                text: safeText,
                timestamp: Date.now()
            });
            return;
        }

        if (data.type === "admin_command") {
            if (String(ws.playerId) !== ADMIN_USER_ID) {
                send(ws, { type: "error", message: "Unauthorized." });
                return;
            }

            let commandString = String(data.commandString || "").trim();
            if (!commandString || commandString.charAt(0) !== ";") return;

            const parts = commandString.split(/\s+/);
            const commandName = String(parts[0] || "").toLowerCase();

            if (commandName === ";staff") {
                const targetName = parts[1];
                const tag = parts.slice(2).join(" ").trim();
                if (!targetName || !tag) return;

                const target = findPlayerInRoom(room, targetName);
                if (!target) return;

                room.staffTags.set(String(target.playerId), {
                    displayName: target.displayName,
                    tag: tag.slice(0, 40)
                });

                broadcast(ws.roomId, {
                    type: "staff_update",
                    action: "set",
                    playerId: String(target.playerId),
                    tag: tag.slice(0, 40),
                    timestamp: Date.now()
                });
                return;
            }

            if (commandName === ";unstaff") {
                const targetName = parts[1];
                if (!targetName) return;

                const target = findPlayerInRoom(room, targetName);
                if (!target) return;

                room.staffTags.delete(String(target.playerId));

                broadcast(ws.roomId, {
                    type: "staff_update",
                    action: "remove",
                    playerId: String(target.playerId),
                    timestamp: Date.now()
                });
                return;
            }

            broadcast(ws.roomId, {
                type: "admin_sync",
                commandString,
                adminUserId: ADMIN_USER_ID,
                timestamp: Date.now()
            });
            return;
        }
    });

    ws.on("close", () => removeClientFromRoom(ws));
    ws.on("error", () => removeClientFromRoom(ws));
});

server.listen(PORT, "0.0.0.0", () => {
    console.log(`ZERO CHAT relay listening on port ${PORT}`);
});
