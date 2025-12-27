import asyncio
from redis_client.initialize import redisClient
from database_client.initialize import DatabaseClient
from database_client.manager import DBManager
from aiohttp import web
from api.handler import API
import signal




async def main():
    # Initialize Redis
    redis = redisClient()
    await redis.initialize()
    print("[MAIN] Redis initialized")
    
    # Initialize Database
    db = DatabaseClient()
    await db.initialize()
    print("[MAIN] Database connected")
    
    # Create DBManager worker
    db_manager = DBManager(
        redis_client=redis,
        db_client=db,
        poll_interval=1.0
    )
    
    # Get all existing rooms from DB
    from database_client.models import Room
    from sqlalchemy import select
    async with db.get_session() as session:
        stmt = select(Room.room_id)
        result = await session.execute(stmt)
        existing_rooms = [row[0] for row in result.all()]
    
    if existing_rooms:
        print(f"[MAIN] Found {len(existing_rooms)} existing rooms")
        
        # Initialize consumer groups
        await db_manager.initialize_consumer_groups(existing_rooms)
        
        # Restore messages from DB to Redis
        print("[MAIN] Restoring messages from DB to Redis...")
        for room_id in existing_rooms:
            count = await db_manager.restore_messages_from_db(room_id, limit=50)
            if count > 0:
                print(f"[MAIN] Restored {count} messages for room {room_id}")
    
    # Start DBManager worker in background
    worker_task = asyncio.create_task(db_manager.run(existing_rooms))
    print("[MAIN] DBManager worker started")
    
    # Create and start API server
    global app
    app = API(redis, "localhost", 8080)
    app.db = db  # Attach DB to API
    app.db_manager = db_manager  # Attach DBManager to API
    app.worker_task = worker_task  # Attach worker task for cleanup
    await app.run()


    loop= asyncio.get_event_loop()

    def signal_handler(sig):
        asyncio.create_task(app.cleanup())
        loop.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler, sig)

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await app.cleanup()

if __name__ == '__main__':
    asyncio.run(main())
