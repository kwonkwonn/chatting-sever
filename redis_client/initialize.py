from glide import (
    GlideClientConfiguration, 
    NodeAddress, 
    GlideClient, 
    StreamReadOptions,
    StreamReadGroupOptions,
    StreamGroupOptions,
    MaxId, 
    MinId,
    StreamTrimOptions
)
import asyncio

class redisClient():
    config = None
    def __init__ (self):
        self.client = None

        addresses = [
            NodeAddress("localhost", 6379),
        ]
        self.config = GlideClientConfiguration(addresses  , request_timeout=500)  # 500ms timeout
        
    async def initialize(self):
        if self.config is None: 
            print("Configuration is not set.")
            return
        self.client= await GlideClient.create(self.config)
 
        
    """
    Utilize Redis Stream service for robust message pass.
    Stream directly go to RDBS manager. 
    """
    async def XAdd(self, key: str, value: list[str]):
        """Add a chat entry to Redis Stream.
        Expects value = [user, message].
        """
        if not isinstance(value, list) or len(value) < 2:
            raise ValueError("XAdd expects value as [user, message]")

        user = value[0]
        msg = value[1]
        data = await self.client.xadd(
            key,
            [
                ("user", user),
                ("message", msg),
            ],
        )
        print(f"[XADD] key={key} user={user} msg={msg} -> {data}")
        #TODO: return 값 규칙 만들기 
        return data

    """`
    Xread read upto 50 of messages from redis stream.
    """
    async def XRead(self, key, length=50):
        data = await self.client.xread({key: "0-0"}, StreamReadOptions(count=length))
        print(f"[XREAD] key={key} count<= {length} -> {data}")
        return data 

    """
    Xlen returns count of messages from stream.
    if messages are less than 50, need to pull from RDBS.
    """
    async def XLen(self, key)-> int:
        return await self.client.xlen(key)
    
    """
    XRevRange reads messages in reverse order (newest first).
    Read latest 50 messages from stream.
    """
    async def XRevRange(self, key, count=50):
        # + = latest, - = oldest
        data = await self.client.xrevrange(
            key, 
            MaxId(),  
            MinId(),  
            count=count
        )
        print(f"[XREVRANGE] key={key} count<={count} -> {data}")
        return data
    
    """
    XGroupCreate creates a consumer group for a stream.
    Used to enable reliable message processing with ACK.
    """
    async def XGroupCreate(self, key: str, group_name: str, start_id: str = "0-0", mkstream: bool = True):
        try:
            # Use StreamGroupOptions with make_stream=True
            options = StreamGroupOptions(make_stream=mkstream) if mkstream else None
            result = await self.client.xgroup_create(
                key,
                group_name,
                start_id,
                options=options
            )
            print(f"[XGROUP CREATE] key={key} group={group_name} start={start_id} mkstream={mkstream} -> {result}")
            return result
        except Exception as e:
            # Group may already exist
            if "BUSYGROUP" in str(e):
                print(f"[XGROUP CREATE] Group {group_name} already exists for {key}")
                return None
            raise
    
    """
    XReadGroup reads messages from stream as part of a consumer group.
    Returns unacknowledged messages for processing.
    """
    async def XReadGroup(self, group_name: str, consumer_name: str, keys_and_ids: dict, count: int = 10):
        print(f"[XREADGROUP DEBUG] group={group_name}, consumer={consumer_name}, keys_and_ids={keys_and_ids}")
        options = StreamReadGroupOptions(count=count)
        print(f"[XREADGROUP DEBUG] options={options}")
        data = await self.client.xreadgroup(
            keys_and_ids,
            group_name,    # Second param: group name
            consumer_name, # Third param: consumer name  
            options        # Fourth param: options
        )
        print(f"[XREADGROUP] group={group_name} consumer={consumer_name} count<={count} -> {len(data) if data else 0} streams")
        return data
    
    """
    XAck acknowledges that a message has been processed.
    Required for consumer group reliability.
    """
    async def XAck(self, key: str, group_name: str, msg_ids: list[str]):
        result = await self.client.xack(key, group_name, msg_ids)
        print(f"[XACK] key={key} group={group_name} ids={msg_ids} -> {result} acked")
        return result
    
    """
    XTrim removes old messages from stream.
    Keep only the latest N messages to save memory.
    """
    async def XTrim(self, key: str, max_len: int = 50):
        result = await self.client.xtrim(
            key,
            StreamTrimOptions(exact=False, limit=max_len, method="maxlen")
        )
        print(f"[XTRIM] key={key} max_len={max_len} -> {result} trimmed")
        return result


    """
    Redis pub/sub sends each packets for subscriber.
    It only sends for quick trigger only for notification, message updates for member in chat.
    """
