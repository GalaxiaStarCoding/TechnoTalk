#!/usr/bin/env python3
"""
TechnoTalk Bridge — WebSocket relay between the TechnoTalk web client
and a real TeamTalk5 server (via the official TeamTalkPy SDK).

Protocol contract: see BRIDGE_SPEC.md in the TechnoTalk app repo.

The browser connects to wss://<this-host> and sends JSON command frames;
this server translates them to TeamTalk5 SDK calls and pushes JSON event
frames back to all connected clients.

If the native TeamTalk5 SDK / TeamTalkPy is not installed, the bridge falls
back to a built-in DEMO backend so the client can still connect and exercise
presence/roster/mute features without a real TT5 server. This lets you deploy
to Render immediately and switch on the real backend later (see README.md).
"""
import asyncio
import json
import os
import logging
from datetime import datetime, timezone

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("technotalk-bridge")

PORT = int(os.environ.get("PORT", "8080"))

# ---------------------------------------------------------------------------
# TeamTalk5 backend
# ---------------------------------------------------------------------------
# Try to load the official TeamTalkPy wrapper. If the native TeamTalk5 DLL/SO
# is not present (e.g. first deploy), fall back to a demo backend.
try:
    from teamtalk import TeamTalkServer, ClientEvent  # noqa: F401
    HAVE_TT5 = True
    log.info("TeamTalkPy loaded — real TeamTalk5 backend available")
except Exception as exc:
    HAVE_TT5 = False
    log.warning("TeamTalkPy unavailable (%s) — running in DEMO mode", exc)


class DemoBackend:
    """In-memory backend that mimics a TeamTalk5 server for testing."""

    def __init__(self):
        self.users = {}  # ws_id -> user dict
        self.channels = [
            {"id": "root", "name": "Root", "parent": None, "topic": "Server lobby"},
            {"id": "general", "name": "General", "parent": "root", "topic": "General voice"},
        ]

    async def login(self, ws_id, creds):
        self.users[ws_id] = {
            "id": ws_id,
            "nickname": creds.get("nickname") or creds.get("username") or "Guest",
            "username": creds.get("username", ""),
            "channel": "general",
            "role": "default",
            "muted": False,
            "deafened": False,
            "hand_raised": False,
            "speaking": False,
        }
        return self.users[ws_id]

    async def logout(self, ws_id):
        self.users.pop(ws_id, None)

    async def join_channel(self, ws_id, channel_id):
        u = self.users.get(ws_id)
        if u:
            u["channel"] = channel_id

    async def set_state(self, ws_id, key, value):
        u = self.users.get(ws_id)
        if u:
            u[key] = value

    def roster(self):
        return list(self.users.values())

    def channels_list(self):
        return self.channels


class TeamTalkBackend:
    """Real TeamTalk5 backend via TeamTalkPy.

    TODO: wire the SDK calls. Sketch:

        self.srv = TeamTalkServer()
        self.srv.connect(host, tcp_port, udp_port, ...)
        self.srv.login(nickname, username, password, ...)
        # then in an event loop poll srv.get_event() and translate
        # ClientEvent_* into the JSON frames below.
    See the TeamTalkPy examples in the TeamTalk5 repo and BRIDGE_SPEC.md.
    """

    def __init__(self):
        raise NotImplementedError("Wire TeamTalkPy SDK calls here (see README).")


backend = DemoBackend()
clients = {}  # ws_id -> websocket


def _now():
    return datetime.now(timezone.utc).isoformat()


async def send(ws, obj):
    await ws.send(json.dumps(obj))


async def broadcast(obj):
    dead = []
    for ws_id, ws in list(clients.items()):
        try:
            await ws.send(json.dumps(obj))
        except Exception:
            dead.append(ws_id)
    for ws_id in dead:
        clients.pop(ws_id, None)


async def push_roster():
    await broadcast({"type": "users", "users": backend.roster()})


async def handle(ws):
    ws_id = str(id(ws))
    clients[ws_id] = ws
    log.info("client connected id=%s", ws_id)
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await send(ws, {"type": "error", "message": "invalid json"})
                continue
            mtype = msg.get("type")
            if mtype == "login":
                user = await backend.login(ws_id, msg)
                await send(ws, {
                    "type": "connected",
                    "user": user,
                    "server_info": {
                        "name": msg.get("label", "TechnoTalk Bridge"),
                        "host": msg.get("domain"),
                        "tcp_port": msg.get("tcp_port", 10333),
                        "udp_port": msg.get("udp_port", 10333),
                    },
                })
                await send(ws, {"type": "channels", "channels": backend.channels_list()})
                await push_roster()
            elif mtype == "join_channel":
                await backend.join_channel(ws_id, msg.get("channel_id"))
                await push_roster()
            elif mtype == "set_tx_mute":
                await backend.set_state(ws_id, "muted", bool(msg.get("muted", False)))
                await push_roster()
            elif mtype == "set_master_mute":
                await backend.set_state(ws_id, "deafened", bool(msg.get("muted", False)))
                await push_roster()
            elif mtype == "raise_hand":
                await backend.set_state(ws_id, "hand_raised", bool(msg.get("raised", False)))
                await push_roster()
            elif mtype == "send_chat":
                u = backend.users.get(ws_id)
                if u:
                    await broadcast({
                        "type": "chat",
                        "channel": u["channel"],
                        "user": u["nickname"],
                        "text": msg.get("text", ""),
                        "timestamp": _now(),
                    })
            elif mtype == "ping":
                await send(ws, {"type": "pong"})
            else:
                await send(ws, {"type": "error", "message": f"unknown type {mtype}"})
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await backend.logout(ws_id)
        clients.pop(ws_id, None)
        await push_roster()
        log.info("client disconnected id=%s", ws_id)


async def main():
    log.info("TechnoTalk bridge listening on :%s (TT5 backend=%s)", PORT, HAVE_TT5)
    async with websockets.serve(handle, "0.0.0.0", PORT, ping_interval=20):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
