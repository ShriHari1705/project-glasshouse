import json
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from transformers import pipeline
from loguru import logger

# Configuration
INPUT_TOPIC = "social.sentiment.raw"
OUTPUT_TOPIC = "social_enriched"
KAFKA_BROKER = "redpanda:9092"
MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment"

async def process_sentiment():
    # Initialize the "Brain"
    # RoBERTa is perfect for short headlines like RSS
    sentiment_task = pipeline("sentiment-analysis", model=MODEL_NAME, device=-1)
    
    consumer = AIOKafkaConsumer(INPUT_TOPIC, bootstrap_servers=KAFKA_BROKER, group_id="sentiment_group")
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    
    await consumer.start()
    await producer.start()
    
    logger.info("Sentiment Worker live. Processing RSS headlines...")

    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            
            # Use Title (Primary) and Summary (Secondary) for context
            text_to_score = f"{data.get('title', '')} {data.get('summary', '')}".strip()
            
            if not text_to_score:
                continue

            # Run Inference
            result = sentiment_task(text_to_score[:512])[0] # Truncate to model limits
            
            # Map RoBERTa labels to scores (0 to 1)
            # LABEL_0: Negative, LABEL_1: Neutral, LABEL_2: Positive
            label_map = {"LABEL_0": 0.0, "LABEL_1": 0.5, "LABEL_2": 1.0}
            score = label_map.get(result['label'], 0.5)

            # Build enriched payload for ClickHouse
            enriched = {
                "title": data.get("title", ""),
                "text": data.get("summary", ""),
                "subreddit": "RSS_FEED", # Placeholder for the Silver layer schema
                "created_utc": data.get("published", ""), 
                "post_id": data.get("link", ""),
                "sentiment_score": float(score),
                "sentiment_label": result['label']
            }

            await producer.send_and_wait(OUTPUT_TOPIC, json.dumps(enriched).encode('utf-8'))
            logger.info(f"Scored: {enriched['title'][:40]}... -> {score}")

    except Exception as e:
        logger.error(f"Worker error: {e}")
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(process_sentiment())