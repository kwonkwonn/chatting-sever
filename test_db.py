"""
Database initialization and ORM test script

Steps:
1. Test connection to PostgreSQL
2. Create tables from ORM models
3. Test basic CRUD operations
4. Verify relationships between Room and Message
"""

import asyncio
from database_client.initialize import DatabaseClient
from database_client.models import Room, Message
from sqlalchemy import select
from datetime import datetime


async def test_connection():
    """Step 1: Test database connection"""
    print("\n=== Testing Database Connection ===")
    db = DatabaseClient()
    await db.initialize()
    await db.test_connection()
    return db


async def create_schema(db: DatabaseClient):
    """Step 2: Create tables"""
    print("\n=== Creating Database Schema ===")
    await db.create_tables()
    print("Tables: rooms, messages created")


async def test_orm_crud(db: DatabaseClient):
    """Step 3: Test ORM CRUD operations"""
    print("\n=== Testing ORM CRUD ===")
    
    async with db.get_session() as session:
        # CREATE: Insert test room
        test_room = Room(
            room_id="test-room-001",
            room_name="테스트 채팅방"
        )
        session.add(test_room)
        await session.commit()
        print(f"✓ Created room: {test_room.room_id}")
        
        # CREATE: Insert test messages
        messages = [
            Message(
                room_id="test-room-001",
                user_id="user1",
                message="안녕하세요!",
                redis_msg_id="1234567890-0"
            ),
            Message(
                room_id="test-room-001",
                user_id="user2",
                message="반갑습니다!",
                redis_msg_id="1234567891-0"
            ),
            Message(
                room_id="test-room-001",
                user_id="user1",
                message="잘 부탁드립니다.",
                redis_msg_id="1234567892-0"
            )
        ]
        session.add_all(messages)
        await session.commit()
        print(f"✓ Created {len(messages)} messages")
        
        # READ: Query all messages in room
        stmt = select(Message).where(Message.room_id == "test-room-001").order_by(Message.created_at)
        result = await session.execute(stmt)
        messages_list = result.scalars().all()
        
        print(f"\n✓ Retrieved {len(messages_list)} messages:")
        for msg in messages_list:
            print(f"  [{msg.created_at}] {msg.user_id}: {msg.message}")
        
        # READ: Query with relationship
        stmt = select(Room).where(Room.room_id == "test-room-001")
        result = await session.execute(stmt)
        room = result.scalar_one()
        
        # Force eager load of messages relationship
        await session.refresh(room, ["messages"])
        
        print(f"\n✓ Room relationship test:")
        print(f"  Room: {room.room_name}")
        print(f"  Messages count: {len(room.messages)}")
        
        # UPDATE: Modify message
        first_msg = messages_list[0]
        first_msg.message = "안녕하세요! (수정됨)"
        await session.commit()
        print(f"\n✓ Updated message: {first_msg.message}")
        
        # DELETE: Remove one message
        await session.delete(messages_list[-1])
        await session.commit()
        print(f"✓ Deleted message")
        
        # Verify deletion
        stmt = select(Message).where(Message.room_id == "test-room-001")
        result = await session.execute(stmt)
        remaining = result.scalars().all()
        print(f"✓ Remaining messages: {len(remaining)}")


async def test_unique_constraint(db: DatabaseClient):
    """Test redis_msg_id unique constraint"""
    print("\n=== Testing Unique Constraint ===")
    
    async with db.get_session() as session:
        try:
            # Try to insert duplicate redis_msg_id
            duplicate_msg = Message(
                room_id="test-room-001",
                user_id="user3",
                message="중복 메시지",
                redis_msg_id="1234567890-0"  # Already exists
            )
            session.add(duplicate_msg)
            await session.commit()
            print("✗ Unique constraint failed - duplicate was inserted!")
        except Exception as e:
            await session.rollback()
            print(f"✓ Unique constraint working: {type(e).__name__}")


async def cleanup(db: DatabaseClient):
    """Clean up test data"""
    print("\n=== Cleanup ===")
    async with db.get_session() as session:
        # Delete all test data
        stmt = select(Room).where(Room.room_id == "test-room-001")
        result = await session.execute(stmt)
        room = result.scalar_one_or_none()
        if room:
            await session.delete(room)
            await session.commit()
            print("✓ Test data cleaned")
    
    await db.close()


async def main():
    """Run all tests"""
    try:
        # Initialize and connect
        db = await test_connection()
        
        # Drop and recreate schema for clean state
        await db.drop_tables()
        await db.create_tables()
        
        # Test CRUD
        await test_orm_crud(db)
        
        # Test constraints
        await test_unique_constraint(db)
        
        # Cleanup
        await cleanup(db)
        
        print("\n=== All Tests Passed ✓ ===\n")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
