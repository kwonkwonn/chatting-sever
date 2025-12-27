"""
DBManager - Background worker for persisting Redis Stream messages to PostgreSQL

Architecture:
1. Redis Stream acts as temporary message queue (max 50 messages per room)
2. DBManager runs as background task, consuming messages via Consumer Group
3. Messages are written to PostgreSQL for permanent storage
4. After successful DB write, messages are ACKed and Stream is trimmed

Consumer Group benefits:
- Reliability: Messages aren't lost if worker crashes
- ACK-based: Only removes messages after confirmed DB write
- Multi-worker ready: Can scale to multiple DBManager instances
"""

import asyncio
import time
from datetime import datetime
from typing import Optional
from sqlalchemy import select
from database_client.initialize import DatabaseClient
from database_client.models import Room, Message
from redis_client.initialize import redisClient


def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] [{level}] [DBManager] {msg}")


class DBManager:
    def __init__(
        self, 
        redis_client: redisClient, 
        db_client: DatabaseClient,
        group_name: str = "db-persist-group",
        consumer_name: str = "db-worker-1",
        poll_interval: float = 1.0,
    ):
        """
        Initialize DBManager worker.
        
        Args:
            redis_client: Redis client for Stream operations
            db_client: Database client for PostgreSQL
            group_name: Consumer group name
            consumer_name: This worker's unique consumer name
            poll_interval: Seconds between polling Redis Stream
        """
        self.redis = redis_client
        self.db = db_client
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.poll_interval = poll_interval
        self.running = False
    
    async def restore_messages_from_db(self, room_id: str, limit: int = 50):
        """
        Restore recent messages from DB to Redis Stream.
        Called on server startup to populate Redis with existing data.
        """
        async with self.db.get_session() as session:
            # Get latest N messages from DB
            stmt = (
                select(Message)
                .where(Message.room_id == room_id)
                .order_by(Message.created_at.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            messages = result.scalars().all()
            
            if not messages:
                log(f"No messages to restore for room {room_id}")
                return 0
            
            # Add messages to Redis in chronological order
            restored_count = 0
            for msg in reversed(messages):  # Oldest first
                try:
                    await self.redis.XAdd(room_id, [msg.user_id, msg.message])
                    restored_count += 1
                except Exception as e:
                    log(f"Error restoring message {msg.id}: {e}", "WARN")
            
            log(f"Restored {restored_count} messages to Redis for room {room_id}")
            return restored_count
    
    async def initialize_consumer_groups(self, room_ids: list[str]):
        """
        Create consumer groups for all active rooms.
        Should be called on server startup.
        """
        log(f"Initializing consumer groups for {len(room_ids)} rooms")
        for room_id in room_ids:
            stream_key = room_id  # Stream key = room_id
            await self.redis.XGroupCreate(stream_key, self.group_name, start_id="$")
            log(f"Consumer group '{self.group_name}' initialized for room: {room_id}")
    
    async def ensure_room_exists(self, room_id: str, room_name: str = None) -> Room:
        """
        Ensure room exists in database, create if missing.
        """
        async with self.db.get_session() as session:
            stmt = select(Room).where(Room.room_id == room_id)
            result = await session.execute(stmt)
            room = result.scalar_one_or_none()
            
            if not room:
                room = Room(
                    room_id=room_id,
                    room_name=room_name or f"Room {room_id}"
                )
                session.add(room)
                await session.commit()
                log(f"Created room: {room_id}")
            
            return room
    
    async def process_message(self, room_id: str, msg_id: str, msg_data: dict):
        """
        Process a single message: save to DB and ACK.
        
        Args:
            room_id: Room stream key
            msg_id: Redis message ID (e.g., b"1234567890-0" or "1234567890-0")
            msg_data: Message fields {b'user': b'user1', b'message': b'hello'}
        """
        start_time = time.time()
        try:
            # Decode msg_id if it's bytes
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode('utf-8')
            
            # Decode message data
            user_id = msg_data.get(b'user', b'').decode('utf-8')
            message_text = msg_data.get(b'message', b'').decode('utf-8')
            
            if not user_id or not message_text:
                log(f"Invalid message format for {msg_id}: {msg_data}", "WARN")
                await self.redis.XAck(room_id, self.group_name, [msg_id])
                return
            
            await self.ensure_room_exists(room_id)
            
            async with self.db.get_session() as session:
                stmt = select(Message).where(Message.redis_msg_id == msg_id)
                result = await session.execute(stmt)
                existing = result.scalar_one_or_none()
                

                new_msg = Message(
                    room_id=room_id,
                    user_id=user_id,
                    message=message_text,
                    redis_msg_id=msg_id
                )
                session.add(new_msg)
                await session.commit()
                
                await self.redis.XAck(room_id, self.group_name, [msg_id])
                
        except Exception as e:
            log(f"Error processing message {msg_id}: {e}", "ERROR")
            # Don't ACK on error - message will be reprocessed
            import traceback
            traceback.print_exc()
    
    async def process_room_stream(self, room_id: str):
        """
        Process all pending messages for a single room.
        """
        try:
            stream_data = await self.redis.XReadGroup(
                group_name=self.group_name,
                consumer_name=self.consumer_name,
                keys_and_ids={room_id: ">"},  # dict with room_id -> ">" for new messages
                count=10  # Process 10 messages at a time
            )
            
            if not stream_data:
                return
            
            for stream_key, messages in stream_data.items():
                if isinstance(messages, dict):
                    msg_count = len(messages)
                    for msg_id, msg_data in messages.items():
                        # Convert msg_data from list of tuples to dict
                        msg_dict = {field: value for field, value in msg_data}
                        await self.process_message(room_id, msg_id, msg_dict)
            
            stream_len = await self.redis.XLen(room_id)
            if stream_len > 50:
                log(f"Trimming stream {room_id} from {stream_len} to 50 messages")
                await self.redis.XTrim(room_id, max_len=50)
                
        except Exception as e:
            log(f"Error processing room {room_id}: {e}", "ERROR")
            import traceback
            traceback.print_exc()
    
    async def run(self, initial_room_ids: list[str] = None):
        """
        Main worker loop - polls all rooms and persists messages.
        Dynamically discovers new rooms from database.
        
        Args:
            initial_room_ids: Initial list of room IDs (optional)
        """
        from sqlalchemy import select
        
        self.running = True
        
        active_rooms = set(initial_room_ids) if initial_room_ids else set()
        log(f"Monitoring {len(active_rooms)} initial rooms: {list(active_rooms)}")
        
        while self.running:
            try:
                async with self.db.get_session() as session:
                    stmt = select(Room.room_id)
                    result = await session.execute(stmt)
                    db_rooms = set(row[0] for row in result.all())
                
                new_rooms = db_rooms - active_rooms
                if new_rooms:
                    await self.initialize_consumer_groups(list(new_rooms))
                    active_rooms.update(new_rooms)
                
                deleted_rooms = active_rooms - db_rooms
                if deleted_rooms:
                    active_rooms -= deleted_rooms
                
                for room_id in active_rooms:
                    await self.process_room_stream(room_id)
                
                await asyncio.sleep(self.poll_interval)
                
            except asyncio.CancelledError:
                log("Worker cancelled")
                self.running = False
                break
            except Exception as e:
                log(f"Unexpected error in main loop: {e}", "ERROR")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(self.poll_interval)
        
        log("Worker stopped")
    
    def stop(self):
        self.running = False
