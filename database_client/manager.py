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
from typing import Optional
from sqlalchemy import select
from database_client.initialize import DatabaseClient
from database_client.models import Room, Message
from redis_client.initialize import redisClient


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
    
    async def initialize_consumer_groups(self, room_ids: list[str]):
        """
        Create consumer groups for all active rooms.
        Should be called on server startup.
        """
        print(f"[DBManager] Initializing consumer groups for {len(room_ids)} rooms")
        for room_id in room_ids:
            stream_key = room_id  # Stream key = room_id
            await self.redis.XGroupCreate(stream_key, self.group_name, start_id="$")
    
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
                print(f"[DBManager] Created room: {room_id}")
            
            return room
    
    async def process_message(self, room_id: str, msg_id: str, msg_data: dict):
        """
        Process a single message: save to DB and ACK.
        
        Args:
            room_id: Room stream key
            msg_id: Redis message ID (e.g., "1234567890-0")
            msg_data: Message fields {b'user': b'user1', b'message': b'hello'}
        """
        try:
            # Decode message data
            user_id = msg_data.get(b'user', b'').decode('utf-8')
            message_text = msg_data.get(b'message', b'').decode('utf-8')
            
            if not user_id or not message_text:
                print(f"[DBManager] Invalid message format: {msg_data}")
                await self.redis.XAck(room_id, self.group_name, [msg_id])
                return
            
            # Ensure room exists
            await self.ensure_room_exists(room_id)
            
            # Save to database
            async with self.db.get_session() as session:
                # Check if message already exists (idempotency)
                stmt = select(Message).where(Message.redis_msg_id == msg_id)
                result = await session.execute(stmt)
                existing = result.scalar_one_or_none()
                
                if existing:
                    print(f"[DBManager] Message {msg_id} already in DB, skipping")
                else:
                    new_msg = Message(
                        room_id=room_id,
                        user_id=user_id,
                        message=message_text,
                        redis_msg_id=msg_id
                    )
                    session.add(new_msg)
                    await session.commit()
                    print(f"[DBManager] Saved to DB: {room_id} | {user_id}: {message_text[:30]}")
                
                # ACK the message
                await self.redis.XAck(room_id, self.group_name, [msg_id])
                
        except Exception as e:
            print(f"[DBManager] Error processing message {msg_id}: {e}")
            # Don't ACK on error - message will be reprocessed
            import traceback
            traceback.print_exc()
    
    async def process_room_stream(self, room_id: str):
        """
        Process all pending messages for a single room.
        """
        try:
            # Read messages from consumer group
            # ">" means read only new messages not yet delivered to this consumer
            stream_data = await self.redis.XReadGroup(
                group_name=self.group_name,
                consumer_name=self.consumer_name,
                keys_and_ids={room_id: ">"},  # dict with room_id -> ">" for new messages
                count=10  # Process 10 messages at a time
            )
            
            if not stream_data:
                return
            
            # Process each message
            for stream_key, messages in stream_data.items():
                if isinstance(messages, list):
                    for msg_id, msg_data in messages:
                        await self.process_message(room_id, msg_id, msg_data)
            
            # Trim stream to keep only latest 50 messages
            stream_len = await self.redis.XLen(room_id)
            if stream_len > 50:
                await self.redis.XTrim(room_id, max_len=50)
                
        except Exception as e:
            print(f"[DBManager] Error processing room {room_id}: {e}")
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
        print(f"[DBManager] Starting worker: {self.consumer_name}")
        
        # Start with initial rooms or empty list
        active_rooms = set(initial_room_ids) if initial_room_ids else set()
        print(f"[DBManager] Monitoring {len(active_rooms)} initial rooms: {list(active_rooms)}")
        
        while self.running:
            try:
                # ① Discover new rooms from database every poll
                async with self.db.get_session() as session:
                    stmt = select(Room.room_id)
                    result = await session.execute(stmt)
                    db_rooms = set(row[0] for row in result.all())
                
                # ② Add new rooms detected
                new_rooms = db_rooms - active_rooms
                if new_rooms:
                    print(f"[DBManager] Discovered {len(new_rooms)} new rooms: {new_rooms}")
                    await self.initialize_consumer_groups(list(new_rooms))
                    active_rooms.update(new_rooms)
                
                # ③ Remove deleted rooms (optional)
                deleted_rooms = active_rooms - db_rooms
                if deleted_rooms:
                    print(f"[DBManager] Removing {len(deleted_rooms)} deleted rooms: {deleted_rooms}")
                    active_rooms -= deleted_rooms
                
                # ④ Process each active room's stream
                for room_id in active_rooms:
                    await self.process_room_stream(room_id)
                
                # Wait before next poll
                await asyncio.sleep(self.poll_interval)
                
            except asyncio.CancelledError:
                print(f"[DBManager] Worker cancelled")
                self.running = False
                break
            except Exception as e:
                print(f"[DBManager] Unexpected error in main loop: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(self.poll_interval)
        
        print(f"[DBManager] Worker stopped")
    
    def stop(self):
        """Stop the worker loop"""
        self.running = False
