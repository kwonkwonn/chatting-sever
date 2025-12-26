"""
Integration Test - Send messages via WebSocket and verify DB persistence
"""
import asyncio
import aiohttp
import json

async def test_integration():
    room_id = "32f39a26-a7f4-4b33-8fa1-a3b50f3721a3"
    user_id = "test-user-1"
    
    print("\n=== Integration Test ===\n")
    
    # Connect to WebSocket
    async with aiohttp.ClientSession() as session:
        ws_url = f"http://localhost:8080/ws/{room_id}/{user_id}".replace("http://", "ws://")
        
        print(f"Step 1: Connecting to WebSocket: {ws_url}")
        async with session.ws_connect(ws_url) as ws:
            print(f"✓ Connected")
            
            # Send messages
            messages = [
                "첫 번째 메시지",
                "두 번째 메시지",
                "세 번째 메시지",
            ]
            
            print(f"\nStep 2: Sending {len(messages)} messages")
            for i, msg in enumerate(messages):
                await ws.send_json({"type": "message", "message": msg})
                print(f"  [{i+1}] Sent: {msg}")
                await asyncio.sleep(0.2)
            
            # Wait for worker to process
            print(f"\nStep 3: Waiting 3 seconds for DBManager to persist messages...")
            await asyncio.sleep(3)
        
        # Verify in database
        print(f"\nStep 4: Checking database")
        from database_client.initialize import DatabaseClient
        from database_client.models import Message
        from sqlalchemy import select
        
        db = DatabaseClient()
        await db.initialize()
        
        async with db.get_session() as session:
            stmt = select(Message).where(Message.room_id == room_id)
            result = await session.execute(stmt)
            db_messages = result.scalars().all()
            
            print(f"✓ Found {len(db_messages)} messages in database:")
            for msg in db_messages:
                print(f"  - {msg.user_id}: {msg.message}")
                print(f"    redis_msg_id: {msg.redis_msg_id}")
        
        await db.close()
        
        if len(db_messages) == len(messages):
            print(f"\n✓ Integration Test PASSED ✓")
        else:
            print(f"\n✗ Expected {len(messages)} messages but found {len(db_messages)}")

if __name__ == "__main__":
    asyncio.run(test_integration())
