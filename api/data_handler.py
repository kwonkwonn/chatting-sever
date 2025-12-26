import json
from aiohttp import web

class data_handler:
    async def read(self,request):
        key = await request.text()
        
        try:
            Message= message.unparse(key,1)
            print(Message)
            data_length= await self.DataHandler.XLen(key)
            if (data_length<=50):
                """
                TODO: logic needs to pull from database.
                """
            
            messages=await self.DataHandler.XRead(key)
        

            decoded_msg= message.decode_mapping(messages)

        except Exception as e:
            error = "while processing read chat data, error occured" + str(e)
            print(error)
            return web.Response(text=error)
        
        return web.Response(text= json.dumps(decoded_msg , ensure_ascii=False))


    """
    ws 로 변경할 것  
    """
    async def chat(self ,request):
        txt =await request.text()
        Message= message.unparse(txt,3)

        await self.DataHandler.XAdd(Message[0], Message[1:] )
        # await 
        # need to implement error rule.
        return web.Response(text="good")



class message:
    """ 
    Message consists of three values.
    Room number -> key
    publisher -> uuid(minimum specifier)
    message -> str
    every value will be divided by ':'. 
    """
    @staticmethod
    def unparse(val:str, length:int)->list[str]:
        result= val.split(":")
        if len(result) !=length:
            raise Exception("message length unmatch")
        return result
    
    
    @staticmethod
    def decode_mapping(data)->dict[str, dict[str, dict[str, str]]]:
        """Mapping[bytes, Mapping[bytes, List[List[bytes]]]] → dict"""
        result = {}
        if data is None:
            return []
        for room_id_bytes, messages_dict in data.items():
            room_id = room_id_bytes.decode('utf-8')
            
            decoded_messages = {}
            for msg_id_bytes, fields_list in messages_dict.items():
                msg_id = msg_id_bytes.decode('utf-8')
                
                field_dict = {}
                for field_pair in fields_list:
                    key = field_pair[0].decode('utf-8')
                    value = field_pair[1].decode('utf-8')
                    field_dict[key] = value
                
                decoded_messages[msg_id] = field_dict
            
            result[room_id] = decoded_messages
        
        return result

    @staticmethod
    def decode_revrange(data):
        """Decode xrevrange output.
        Accepts either dict {msg_id: [[b'k', b'v'], ...]} or list[(msg_id, [[b'k', b'v'], ...])].
        Returns list of dicts with keys id/user/message in the order provided (newest→oldest).
        """
        if data is None:
            return []

        def _decode_pair(pair):
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                return None, None
            k_raw, v_raw = pair
            k = k_raw.decode('utf-8') if isinstance(k_raw, (bytes, bytearray)) else k_raw
            v = v_raw.decode('utf-8') if isinstance(v_raw, (bytes, bytearray)) else v_raw
            return k, v

        result = []

        if isinstance(data, dict):
            for msg_id, fields in data.items():
                msg = {"id": msg_id.decode('utf-8') if isinstance(msg_id, (bytes, bytearray)) else msg_id}
                if isinstance(fields, list):
                    for pair in fields:
                        k, v = _decode_pair(pair)
                        if k is not None:
                            msg[k] = v
                result.append(msg)

        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (list, tuple)) and len(item) == 2:
                    msg_id, fields = item
                    msg = {"id": msg_id.decode('utf-8') if isinstance(msg_id, (bytes, bytearray)) else msg_id}
                    if isinstance(fields, list):
                        for pair in fields:
                            k, v = _decode_pair(pair)
                            if k is not None:
                                msg[k] = v
                    result.append(msg)

        return result

class messageBatch:
    """
    Pulls batch message for certain key from redis. 
    If redis fail, pull from RDBS. 
    """
    