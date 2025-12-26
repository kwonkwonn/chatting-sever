from aiohttp import web
import time


class socket_client:
    user_id: str
    ws: web.WebSocketResponse
    last_active: float

    def __init__(self, uid: str):
        self.user_id = uid
        self.ws = None
        self.last_active = time.time()

    async def prepare(self, request: web.Request) -> web.WebSocketResponse:
        self.ws = web.WebSocketResponse()
        await self.ws.prepare(request)
        self.last_active = time.time()
        return self.ws

    async def send_json(self, data: dict):
        if self.ws and not self.is_closed():
            await self.ws.send_json(data)

    def is_closed(self) -> bool:
        return self.ws is None or self.ws.closed or self.ws._writer is None

    async def route_message(self, message: str):
        # placeholder for future routing
        self.last_active = time.time()
        return
