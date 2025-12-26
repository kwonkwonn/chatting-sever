from room_con.socket_client import socket_client


class Room:
    def __init__(self, room_name: str):
        self.room_name = room_name
        self.clients: list[socket_client] = []
    
    def add_client(self, client: socket_client) -> None:
        self.clients.append(client)
        print(f"✓ {client.user_id} joined {self.room_name}")
    
    def remove_client(self, client: socket_client) -> None:
        if client in self.clients:
            self.clients.remove(client)
            print(f"✗ {client.user_id} left {self.room_name}")
    
    def get_info(self) -> str:
        return f"{self.room_name} ({len(self.clients)} users)"
    
    def get_client_count(self) -> int:
        return len(self.clients)
    
    async def broadcast(self, message: dict, exclude_user: str | None = None) -> None:
        for client in self.clients:
            if exclude_user is None or client.user_id != exclude_user:
                await client.send_json(message)


class RoomController:
    def __init__(self, dataHandler=None, redisHandler=None):
        self.rooms: dict[str, Room] = {}
        # logics finding existing rooms from redis and RDS can be added later.
    
    def add_rooms(self, room_name: str, room_index: str) -> str:
        if room_index in self.rooms:
            raise Exception("room already exists")
        self.rooms[room_index] = Room(room_name)
        return "room created"
    
    def get_rooms(self) -> list[dict[str, str]]:
        room_info: list[dict[str, str]] = []
        for key in self.rooms:
            room_info.append({"name": self.rooms[key].room_name, "id": key})
        return room_info




