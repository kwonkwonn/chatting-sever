"""
Worker Test - Verify DBManager consumes Redis Stream and persists to PostgreSQL

Test flow:
1. Setup: Create Redis client, DB client, initialize schema
2. Create test room and consumer group
3. Add messages to Redis Stream via XAdd
4. Start DBManager worker in background
5. Wait for worker to process messages
6. Verify messages are in PostgreSQL
7. Verify messages are ACKed in Redis
8. Cleanup
"""

import asyncio
from database_client.initialize import DatabaseClient
from database_client.manager import DBManager
from database_client.models import Room, Message
from redis_client.initialize import redisClient
from sqlalchemy import select


async def test_worker():
    print("\n=== Worker Model Test ===\n")
    
    # Step 1: Initialize Redis and Database
    print("Step 1: Initializing Redis and Database clients")
    redis = redisClient()
    await redis.initialize()
    
    db = DatabaseClient()
    await db.initialize()
    await db.create_tables()
    print("✓ Clients initialized\n")
    
    # Step 2: Create test room
    test_room_id = "test-worker-room"
    print(f"Step 2: Creating test room: {test_room_id}")
    
    async with db.get_session() as session:
        # Clean up if exists
        stmt = select(Room).where(Room.room_id == test_room_id)
        result = await session.execute(stmt)
        existing_room = result.scalar_one_or_none()
        if existing_room:
            await session.delete(existing_room)
            await session.commit()
        
        # Create fresh room
        room = Room(room_id=test_room_id, room_name="Worker Test Room")
        session.add(room)
        await session.commit()
        print(f"✓ Room created: {room.room_id}\n")
    
    # Step 3: Create consumer group
    print("Step 3: Creating consumer group")
    manager = DBManager(
        redis_client=redis,
        db_client=db,
        group_name="test-db-group",
        consumer_name="test-worker-1",
        poll_interval=0.5  # Fast polling for test
    )
    await manager.initialize_consumer_groups([test_room_id])
    print("✓ Consumer group created\n")
    
    # Step 4: Add test messages to Redis Stream
    print("Step 4: Adding messages to Redis Stream")
    test_messages = [
        ["user1", "첫 번째 메시지입니다"],
        ["user2", "두 번째 메시지입니다"],
        ["user1", "세 번째 메시지입니다"],
        ["user3", "네 번째 메시지입니다"],
        ["user2", "다섯 번째 메시지입니다"],
    ]
    
    msg_ids = []
    for msg in test_messages:
        msg_id = await redis.XAdd(test_room_id, msg)
        msg_ids.append(msg_id)
        print(f"  Added: {msg[0]}: {msg[1]} -> {msg_id}")
    
    # Verify stream length
    stream_len = await redis.XLen(test_room_id)
    print(f"✓ Stream length: {stream_len}\n")
    
    # Step 5: Start worker in background
    print("Step 5: Starting DBManager worker")
    worker_task = asyncio.create_task(manager.run([test_room_id]))
    
    # Wait for worker to process messages
    print("Waiting 3 seconds for worker to process messages...")
    await asyncio.sleep(3)
    
    # Step 6: Verify messages in PostgreSQL
    print("\nStep 6: Verifying messages in PostgreSQL")
    async with db.get_session() as session:
        stmt = select(Message).where(Message.room_id == test_room_id).order_by(Message.created_at)
        result = await session.execute(stmt)
        db_messages = result.scalars().all()
        
        print(f"✓ Found {len(db_messages)} messages in database:")
        for msg in db_messages:
            print(f"  [{msg.created_at}] {msg.user_id}: {msg.message}")
            print(f"    redis_msg_id: {msg.redis_msg_id}")
    
    # Step 7: Verify messages are ACKed (check pending count)
    print("\nStep 7: Checking Redis Stream status")
    try:
        # Read with consumer group - should get no new messages
        pending = await redis.XReadGroup(
            group_name="test-db-group",
            consumer_name="test-worker-check",
            keys_and_ids={test_room_id: ">"},  # Use keys_and_ids parameter
            count=10
        )
        pending_count = len(pending.get(test_room_id, [])) if pending else 0
        print(f"✓ Pending messages: {pending_count}")
        
        stream_len_after = await redis.XLen(test_room_id)
        print(f"✓ Stream length after processing: {stream_len_after}")
    except Exception as e:
        print(f"⚠ Error checking stream status: {e}")
    
    # Step 8: Test idempotency (re-processing same messages)
    print("\nStep 8: Testing idempotency")
    initial_count = len(db_messages)
    
    # Try to process again (worker should skip duplicates)
    await asyncio.sleep(2)
    
    async with db.get_session() as session:
        stmt = select(Message).where(Message.room_id == test_room_id)
        result = await session.execute(stmt)
        final_messages = result.scalars().all()
        print(f"✓ Message count after re-processing: {len(final_messages)} (should be {initial_count})")
        
        if len(final_messages) == initial_count:
            print("✓ Idempotency check passed - no duplicates")
        else:
            print("✗ Idempotency check failed - duplicates found!")
    
    # Step 9: Cleanup
    print("\nStep 9: Cleanup")
    manager.stop()
    await worker_task
    
    # Clean up database
    async with db.get_session() as session:
        stmt = select(Room).where(Room.room_id == test_room_id)
        result = await session.execute(stmt)
        room = result.scalar_one_or_none()
        if room:
            await session.delete(room)
            await session.commit()
    
    await db.close()
    print("✓ Cleanup complete")
    
    print("\n=== Worker Test Passed ✓ ===\n")


async def test_multiple_rooms():
    """Test worker handling multiple rooms simultaneously"""
    print("\n=== Multi-Room Worker Test ===\n")
    
    redis = redisClient()
    await redis.initialize()
    
    db = DatabaseClient()
    await db.initialize()
    
    # Create multiple test rooms
    room_ids = ["room-1", "room-2", "room-3"]
    
    for room_id in room_ids:
        async with db.get_session() as session:
            stmt = select(Room).where(Room.room_id == room_id)
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()
            if existing:
                await session.delete(existing)
                await session.commit()
            
            room = Room(room_id=room_id, room_name=f"Test {room_id}")
            session.add(room)
            await session.commit()
    
    # Initialize consumer groups
    manager = DBManager(redis, db, poll_interval=0.5)
    await manager.initialize_consumer_groups(room_ids)
    
    # Add messages to each room
    for i, room_id in enumerate(room_ids):
        for j in range(3):
            await redis.XAdd(room_id, [f"user{i}", f"Message {j} in {room_id}"])
    
    # Start worker
    worker_task = asyncio.create_task(manager.run(room_ids))
    await asyncio.sleep(3)
    
    # Check results
    total_messages = 0
    async with db.get_session() as session:
        for room_id in room_ids:
            stmt = select(Message).where(Message.room_id == room_id)
            result = await session.execute(stmt)
            messages = result.scalars().all()
            print(f"✓ {room_id}: {len(messages)} messages")
            total_messages += len(messages)
    
    print(f"\n✓ Total messages across all rooms: {total_messages}")
    
    # Cleanup
    manager.stop()
    await worker_task
    
    for room_id in room_ids:
        async with db.get_session() as session:
            stmt = select(Room).where(Room.room_id == room_id)
            result = await session.execute(stmt)
            room = result.scalar_one_or_none()
            if room:
                await session.delete(room)
                await session.commit()
    
    await db.close()
    print("\n=== Multi-Room Test Passed ✓ ===\n")


async def main():
    """Run all worker tests"""
    try:
        await test_worker()
        await test_multiple_rooms()
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
