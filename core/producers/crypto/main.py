import json
import asyncio
from websockets import connect
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel, Field, field_validator
from loguru import logger

class BinanceTrade(BaseModel):
    symbol: str = Field(alias='s')
    price: float = Field(alias='p')
    quantity: float = Field(alias='q')
    timestamp: int = Field(alias='T')

    @field_validator('price', 'quantity', mode='before')
    def validate_floats(cls, v):
        try:
            return float(v)
        except ValueError:
            raise ValueError(f"Invalid numeric value: {v}")

async def stream_binance():
    producer = AIOKafkaProducer(bootstrap_servers='redpanda:9092')
    await producer.start()
    
    url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
    
    async with connect(url) as websocket:
        logger.info("Connected to Binance Firehose")
        while True:
            try:
                data = await websocket.recv()
                raw_json = json.loads(data)
                
                # Validation Step
                trade = BinanceTrade(**raw_json)
                
                await producer.send_and_wait(
                    "market.trades", 
                    trade.model_dump_json().encode('utf-8')
                )
                # Heartbeat for Health Check
                with open("/tmp/heartbeat_crypto", "w") as f:
                    f.write(str(asyncio.get_event_loop().time()))
                    
            except Exception as e:
                logger.error(f"Ingestion Error: {e}")
                with open("/app/logs/ingestion_errors.log", "a") as f:
                    f.write(f"{raw_json}\n")

if __name__ == "__main__":
    asyncio.run(stream_binance())