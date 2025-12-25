import asyncio
import json
import os
from datetime import datetime
from loguru import logger
from pydantic import BaseModel, ValidationError, field_validator
from aiokafka import AIOKafkaProducer
from binance import AsyncClient, BinanceSocketManager

# 1. Validation Schema
class BinanceTrade(BaseModel):
    symbol: str = "BTCUSDT"
    price: float
    quantity: float
    timestamp: datetime
    trade_id: int

    @field_validator('price', 'quantity', mode='before')
    def parse_float(cls, v):
        try:
            return float(v)
        except (ValueError, TypeError):
            raise ValueError("Must be a valid float")

# 2. Setup Error Logging
logger.add("/app/logs/ingestion_errors.log", filter=lambda record: record["level"].name == "ERROR")

async def produce_trades():
    # Kafka Producer Setup
    producer = AIOKafkaProducer(bootstrap_servers=os.getenv("KAFKA_BROKERS", "localhost:9092"))
    await producer.start()
    
    # Binance Setup
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    ts = bm.aggtrade_socket('BTCUSDT')

    logger.info("Connection established. Starting Binance aggTrade stream...")
    
    async with ts as tscm:
        while True:
            msg = await tscm.recv()
            try:
                # Map Binance raw keys to our Schema
                trade_data = BinanceTrade(
                    symbol=msg['s'],
                    price=msg['p'],
                    quantity=msg['q'],
                    timestamp=datetime.fromtimestamp(msg['E'] / 1000.0),
                    trade_id=msg['a']
                )
                
                # Async Kafka Push
                await producer.send("trades_raw", trade_data.model_dump_json().encode('utf-8'))
                
                # Update Health Heartbeat (DataSimp Standard)
                with open("/tmp/heartbeat", "w") as f:
                    f.write(str(datetime.now().timestamp()))

            except (ValidationError, KeyError) as e:
                logger.error(f"Validation Failed: {e} | Raw Msg: {msg}")
            except Exception as e:
                logger.critical(f"System Failure: {e}")

if __name__ == "__main__":
    asyncio.run(produce_trades())