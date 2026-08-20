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
const MAX_STAFF_TAG_LENGTH = 40;

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

//========================================================
// SOCKET HELPERS
//========================================================

function send(ws, payload) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        return false;
    }

    try {
        ws.send(JSON.stringify(payload));
        return true;
    } catch {
        return false;
    }
}

function broadcast(roomId, payload) {
    const room = rooms.get(roomId);

    if (!room) {
        return;
    }

    const encoded = JSON.stringify(payload);

    for (const client of room.clients) {
        if (client.readyState !== WebSocket.OPEN) {
            continue;
        }

        try {
            client.send(encoded);
        } catch {}
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
// STAFF TAGS
//========================================================

function getStaffTag(room, playerId) {
    if (!room || !playerId) {
        return "";
    }

    const entry = room.staffTags.get(
        String(playerId)
    );

    if (!entry) {
        return "";
    }

    return String(entry.tag || "");
}

function getStaffTagList(room) {
    if (!room) {
        return [];
    }

    const result = [];

    for (const [playerId, entry] of room.staffTags) {
        result.push({
            playerId: String(playerId),
            displayName: String(
                entry.displayName || "Player"
            ),
            tag: String(entry.tag || "")
        });
    }

    return result;
}

//========================================================
// PLAYER LOOKUP
//========================================================

function findPlayerInRoom(room, name) {
    if (!room) {
        return null;
    }

    const search = String(
        name || ""
    ).trim().toLowerCase();

    if (!search) {
        return null;
    }

    // Exact username / display name
    for (const client of room.clients) {
        if (
            String(client.username || "")
                .toLowerCase() === search
            ||
            String(client.displayName || "")
                .toLowerCase() === search
        ) {
            return client;
        }
    }

    // Exact UserId
    for (const client of room.clients) {
        if (
            String(client.playerId || "")
                .toLowerCase() === search
        ) {
            return client;
        }
    }

    // Username prefix
    for (const client of room.clients) {
        if (
            String(client.username || "")
                .toLowerCase()
                .startsWith(search)
        ) {
            return client;
        }
    }

    // DisplayName prefix
    for (const client of room.clients) {
        if (
            String(client.displayName || "")
                .toLowerCase()
                .startsWith(search)
        ) {
            return client;
        }
    }

    return null;
}

//========================================================
// ROOM CLEANUP
//========================================================

function removeClientFromRoom(ws) {
    const roomId = ws.roomId;

    if (!roomId) {
        return;
    }

    const room = rooms.get(roomId);

    ws.roomId = null;

    if (!room) {
        return;
    }

    room.clients.delete(ws);

    // Remove stale staff assignment when player leaves.
    if (ws.playerId) {
        room.staffTags.delete(
            String(ws.playerId)
        );
    }

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
    ws.username = null;
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
            data = JSON.parse(
                raw.toString()
            );
        } catch {
            return;
        }

        if (
            !data ||
            typeof data !== "object"
        ) {
            return;
        }

        //================================================
        // JOIN
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

            removeClientFromRoom(ws);

            ws.roomId = roomId;

            ws.playerId = String(
                data.playerId || ""
            ).slice(
                0,
                MAX_PLAYER_ID_LENGTH
            );

            /*
                Supports both:
                username
                displayName
            */

            ws.username = String(
                data.username ||
                data.name ||
                data.displayName ||
                "Player"
            ).slice(
                0,
                MAX_DISPLAY_NAME_LENGTH
            );

            ws.displayName = String(
                data.displayName ||
                data.username ||
                data.name ||
                "Player"
            ).slice(
                0,
                MAX_DISPLAY_NAME_LENGTH
            );

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

        //================================================
        // REQUIRE ROOM
        //================================================

        if (!ws.roomId) {
            return;
        }

        const room = rooms.get(ws.roomId);

        if (!room) {
            return;
        }

        //================================================
        // CHAT
        //================================================

        if (data.type === "chat") {

            const text = String(
                data.text || ""
            ).trim();

            if (!text) {
                return;
            }

            const safeText =
                text.slice(
                    0,
                    MAX_CHAT_LENGTH
                );

            const staffTag =
                getStaffTag(
                    room,
                    ws.playerId
                );

            broadcast(ws.roomId, {
                type: "chat",

                playerId:
                    String(ws.playerId || ""),

                displayName:
                    String(
                        ws.displayName ||
                        "Player"
                    ),

                staffTag,

                text: safeText,

                timestamp: Date.now()
            });

            return;
        }

        //================================================
        // ADMIN COMMAND
        //================================================

        if (data.type === "admin_command") {

            // Server-side admin check.
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

            let commandString = String(
                data.commandString || ""
            ).trim();

            if (!commandString) {
                return;
            }

            if (
                commandString.charAt(0) !== ";"
            ) {
                return;
            }

            commandString =
                commandString.slice(
                    0,
                    MAX_COMMAND_LENGTH
                );

            const parts =
                commandString.split(/\s+/);

            const commandName =
                String(
                    parts[0] || ""
                ).toLowerCase();

            //================================================
            // ;STAFF
            //================================================

            if (commandName === ";staff") {

                const targetName = parts[1];

                const tag = parts
                    .slice(2)
                    .join(" ")
                    .trim();

                if (!targetName || !tag) {

                    send(ws, {
                        type: "error",
                        message:
                            "Usage: ;staff Player Tag"
                    });

                    return;
                }

                const target =
                    findPlayerInRoom(
                        room,
                        targetName
                    );

                if (!target) {

                    send(ws, {
                        type: "error",
                        message:
                            "Player not found."
                    });

                    return;
                }

                const safeTag =
                    tag.slice(
                        0,
                        MAX_STAFF_TAG_LENGTH
                    );

                room.staffTags.set(
                    String(target.playerId),
                    {
                        displayName:
                            target.displayName,

                        tag: safeTag
                    }
                );

                broadcast(ws.roomId, {
                    type: "staff_update",
                    action: "set",

                    playerId:
                        String(target.playerId),

                    displayName:
                        target.displayName,

                    tag: safeTag,

                    timestamp: Date.now()
                });

                return;
            }

            //================================================
            // ;UNSTAFF
            //================================================

            if (commandName === ";unstaff") {

                const targetName = parts[1];

                if (!targetName) {

                    send(ws, {
                        type: "error",
                        message:
                            "Usage: ;unstaff Player"
                    });

                    return;
                }

                const target =
                    findPlayerInRoom(
                        room,
                        targetName
                    );

                if (!target) {

                    send(ws, {
                        type: "error",
                        message:
                            "Player not found."
                    });

                    return;
                }

                room.staffTags.delete(
                    String(target.playerId)
                );

                broadcast(ws.roomId, {
                    type: "staff_update",
                    action: "remove",

                    playerId:
                        String(target.playerId),

                    displayName:
                        target.displayName,

                    timestamp: Date.now()
                });

                return;
            }

            //================================================
            // ADMIN SYNC
            //================================================

            /*
                Commands such as:

                ;hl
                ;unhl
                ;title
                ;untitle
                ;ban

                are sent to every connected client.

                Each client then applies the visual/
                local action to the matching player.
            */

            broadcast(ws.roomId, {
                type: "admin_sync",

                commandString,

                adminUserId:
                    ADMIN_USER_ID,

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
// HEARTBEAT
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
// SHUTDOWN
//========================================================

function shutdown() {

    clearInterval(
        heartbeatInterval
    );

    for (const ws of wss.clients) {

        try {
            ws.close();
        } catch {}
    }

    server.close(() => {
        process.exit(0);
    });
}

process.on(
    "SIGTERM",
    shutdown
);

process.on(
    "SIGINT",
    shutdown
);

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
