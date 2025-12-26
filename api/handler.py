import json
import uuid
from pathlib import Path
import asyncio

from aiohttp import web

from api.data_handler import data_handler, message
from redis_client.initialize import redisClient
from room_con.room import RoomController
from room_con.socket_client import socket_client





class API(data_handler):
    IP: str
    port: int  
    Handler: None 
    DataHandler: redisClient
    RoomHandler:RoomController

    def __init__(self, DataHandler: redisClient, address:str, port:int):
        self.Address = address
        self.port = port
        self.Handler = web.Application()
        self.DataHandler = DataHandler
        self.base_dir = Path(__file__).parent.parent
        self.static_dir = self.base_dir / 'static'
        self.RoomHandler = RoomController()
        self.runner: web.AppRunner | None = None
        self.add_routes()


    async def run(self):
        self.runner = web.AppRunner(self.Handler)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.Address, self.port)
        await site.start()
        print(f"sever started {self.Address ,':',self.port}")


    async def cleanup(self):
        print("[API] shutting down...")
        
        # Stop DBManager worker
        if hasattr(self, 'db_manager'):
            print("[API] stopping DBManager worker...")
            self.db_manager.stop()
            if hasattr(self, 'worker_task'):
                try:
                    await self.worker_task
                except asyncio.CancelledError:
                    pass
        
        # Close database connection
        if hasattr(self, 'db'):
            print("[API] closing database connection...")
            await self.db.close()
        
        # Cleanup web runner
        if self.runner:
            await self.runner.cleanup()
        
        print("[API] cleanup complete")
    
    async def new_client(self, request):
        return web.Response(text=str(uuid.uuid4()))

    async def get_rooms(self, request):
        #read from database.
        rooms = self.RoomHandler.get_rooms()
        to_json = json.dumps(rooms, ensure_ascii=False)
        return web.Response(text=to_json, content_type='application/json')

    async def post_rooms(self, request):
        data = await request.json()
        name = data.get("name")
        if not name:
            return web.json_response({"error": "name is required"}, status=400)

        new_uuid = str(uuid.uuid4())
        try:
            self.RoomHandler.add_rooms(name, new_uuid)
            
            # Save room to database
            if hasattr(self, 'db'):
                from database_client.models import Room
                async with self.db.get_session() as session:
                    room = Room(room_id=new_uuid, room_name=name)
                    session.add(room)
                    await session.commit()
                print(f"[POST_ROOMS] Room {new_uuid} saved to database")
            
            # Add to DBManager consumer groups if worker is running
            if hasattr(self, 'db_manager'):
                await self.db_manager.initialize_consumer_groups([new_uuid])
                print(f"[POST_ROOMS] Consumer group created for {new_uuid}")
                
        except Exception as e:
            print(f"[POST_ROOMS] error: {e}")
            import traceback
            traceback.print_exc()
            return web.json_response({"error": str(e)}, status=400)

        return web.json_response({"id": new_uuid, "name": name})


    async def get_messages(self, request):
        room_id = request.match_info.get("room_id")
        print(f"[GET_MESSAGES] room_id={room_id}")
        try:
            # XRevRange로 최신 50개 역순 조회 (최신→오래된)
            raw = await self.DataHandler.XRevRange(room_id, count=50)
            print(f"[GET_MESSAGES] raw={raw}")
            result = message.decode_revrange(raw)
            # 최신→오래된 순 그대로 반환 (프론트에서 reverse)
            return web.json_response(result)
        except Exception as e:
            print(f"[GET_MESSAGES] error: {e}")
            return web.json_response([], status=200)

    def add_routes(self):
        """ 
        Set of adding routes to the API.
        """
        async def index(request):
            return web.FileResponse(self.static_dir / 'index.html')
        
        self.Handler.add_routes([
            web.get('/', index ) ,
            web.post('/chat',self.chat ),
            web.get('/read',self.read ),
            web.get('/rooms',self.get_rooms),
            web.post('/rooms', self.post_rooms),
            web.get('/rooms/{room_id}/messages', self.get_messages),
            web.get('/ws/{room_id}/{user_id}', self.websocket),
            ])

    async def websocket(self, request: web.Request):
        room_id = request.match_info['room_id']
        user_id = request.match_info['user_id']
        print(f"[WS] connect room={room_id} user={user_id}")

        if room_id not in self.RoomHandler.rooms:
            return web.Response(text='Room not found', status=404)

        room = self.RoomHandler.rooms[room_id]
        client = socket_client(user_id)
        ws = await client.prepare(request)

        room.add_client(client)

        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except Exception:
                        data = {"message": msg.data}
                    text = data.get("message", "")
                    print(f"[WS] recv room={room_id} user={user_id} msg={text}")
                    
                    # Redis에 메시지 저장
                    try:
                        await self.DataHandler.XAdd(room_id, [user_id, text])
                        print(f"[WS] saved to Redis")
                    except Exception as e:
                        print(f"[WS] Redis save error: {e}")
                    
                    # Since web socket and Redis are in the same server, broadcast directly
                    # if we need to scale, we can use Redis pub/sub or other message brokers.
                    await room.broadcast({
                        "type": "message",
                        "user": user_id,
                        "message": text
                    })
                elif msg.type == web.WSMsgType.ERROR:
                    print(f"[WS] error: {ws.exception()}")
        finally:
            room.remove_client(client)

        return ws



    

        



