import asyncio
from redis_client.initialize import redisClient
from aiohttp import web
from api.handler import API
import signal




async def main():
    redis = redisClient()
    await redis.initialize()
    global app
    app = API(redis, "localhost", 8080)
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
