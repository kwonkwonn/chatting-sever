# Chatting Server (Redis Stream + PostgreSQL + aiohttp)

A fast, reliable chat server that uses Redis Streams for real-time buffering and PostgreSQL for durable storage. An async background worker (`DBManager`) consumes Redis Stream messages via Consumer Groups and persists them to the database. The server exposes REST and WebSocket APIs via `aiohttp`.

## Features

- Real-time chat over WebSocket
- Dual storage: Redis (fast, recent 50) + PostgreSQL (durable)
- Consumer Group worker with ACKs to prevent message loss
- Server startup restoration: DB → Redis (last 50 per room)
- REST endpoints for rooms and messages

## Architecture

```
main.py ─┬─ initializes Redis, DB, DBManager
         ├─ restores messages DB → Redis (per room)
         └─ starts aiohttp API and DBManager worker

Redis Client (redis_client/initialize.py)
  ├─ XAdd / XRevRange / XLen / XReadGroup / XAck / XTrim
  └─ Consumer Group support (db-persist-group)

Database (database_client)
  ├─ initialize.py   (Async engine + sessions)
  ├─ models.py       (Room, Message)
  └─ manager.py      (DBManager background worker)

API (api/handler.py)
  ├─ GET  /rooms
  ├─ POST /rooms
  ├─ GET  /rooms/{room_id}/messages
  └─ WS   /ws/{room_id}/{user_id}

Room Controller (room_con/room.py)
  ├─ In-memory Room store (clients, broadcast)
  └─ Synced from DB on GET /rooms
```

## Requirements

- macOS, zsh (tested)
- Python 3.11+
- PostgreSQL 14+ running locally
- Redis or Valkey running on `localhost:6379`

## Setup

```bash
# 1) Create and activate venv
python3 -m venv .venv
source .venv/bin/activate

# 2) Install dependencies (note: file name is 'requirment.txt')
pip install -U pip
pip install -r requirment.txt

# 3) Ensure Redis/Valkey is running
# If using Homebrew Redis:
brew services start redis
# Or run Valkey/Redis container if preferred

# 4) Create database (default DSN below)
# Default DB URL: postgresql+asyncpg://postgres:password@localhost:5432/chatdb
# Create the DB locally (adjust user/password if needed)
createdb chatdb || true
```

### Initialize tables (one-off)

```bash
python3 - <<'PY'
import asyncio
from database_client.initialize import DatabaseClient
async def go():
    db = DatabaseClient()
    await db.initialize()
    await db.create_tables()
    await db.close()
asyncio.run(go())
PY
```

## Run

```bash
python3 main.py
```

Server listens on `http://localhost:8080`.

## API

- `GET /rooms` → List rooms (reads from DB, syncs in-memory)
- `POST /rooms` → Create room
  - Body: `{ "name": "Room Name" }`
- `GET /rooms/{room_id}/messages` → Fetch last 50 messages
  - Tries Redis first, falls back to DB if empty
- `GET /ws/{room_id}/{user_id}` → WebSocket endpoint for chat

### Examples

```bash
# Create a room
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"name":"Quick Test"}' \
  http://localhost:8080/rooms | python3 -m json.tool

# List rooms
curl -s http://localhost:8080/rooms | python3 -m json.tool

# Get messages for a room
curl -s http://localhost:8080/rooms/<room_id>/messages | python3 -m json.tool
```

## WebSocket quick start

Use any WebSocket client (e.g., `websocat`).

```bash
# Install websocat (if needed)
brew install websocat

# Connect and send messages
websocat ws://localhost:8080/ws/<room_id>/<user_id>
# type: {"message":"Hello"}
```

Messages are saved to Redis immediately and broadcast to connected clients. The DBManager worker persists them to PostgreSQL and ACKs the stream entries. Streams are trimmed to keep only the latest 50 entries per room.

## Persistence & Startup Behavior

- Redis Streams are volatile; PostgreSQL is the source of truth.
- On server startup:
  - Existing rooms are loaded from DB
  - Consumer Groups are ensured per room
  - Last 50 messages per room are restored from DB to Redis

## Configuration

- Database DSN default is hardcoded in `database_client/initialize.py`:
  ```python
  postgresql+asyncpg://postgres:password@localhost:5432/chatdb
  ```
  You can change it by editing the file or constructing `DatabaseClient(db_url=...)`.

## Troubleshooting

- ModuleNotFoundError: `glide`
  - The requirements include `valkey-glide` which provides the Glide client.
  - If you still see the error, install the Python glide package:
    ```bash
    pip install glide-python
    ```
- Cannot connect to Redis on 6379
  - Ensure Redis/Valkey is running locally: `brew services start redis`
- DB connection refused
  - Confirm PostgreSQL is running and the DSN matches your local setup;
    update `database_client/initialize.py` if needed.
- Port 8080 already in use
  ```bash
  lsof -ti:8080 | xargs kill -9 2>/dev/null || echo "No process on port 8080"
  ```
- Rooms list empty in UI
  - Verify rooms exist in DB (`POST /rooms` creates and persists).
  - `GET /rooms` reads from DB and syncs RoomController.

## Project Layout

```
chatting_server/
├── main.py
├── requirment.txt
├── api/
│   ├── handler.py
│   └── data_handler.py
├── database_client/
│   ├── initialize.py
│   ├── manager.py
│   └── models.py
├── redis_client/
│   └── initialize.py
├── room_con/
│   ├── room.py
│   └── socket_client.py
└── static/
    └── index.html
```

## Notes

- This project uses async SQLAlchemy + asyncpg.
- DBManager polls every 1s and processes up to 10 messages per cycle.
- Streams are trimmed to 50 messages per room to keep Redis small.
