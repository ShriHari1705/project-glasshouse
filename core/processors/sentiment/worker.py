import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from transformers import pipeline
from loguru import logger

# Initialize the NLP model once (Global)
# This model is optimized for social media 'vibe'
sentiment_task = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest", tokenizer="cardiffnlp/twitter-roberta-base-sentiment-latest")

async def process_sentiment():
    consumer = AIOKafkaConsumer(
        "social_raw",
        bootstrap_servers=os.getenv("KAFKA_BROKERS"),
        group_id="sentiment_worker_group"
    )
    producer = AIOKafkaProducer(bootstrap_servers=os.getenv("KAFKA_BROKERS"))
    
    await consumer.start()
    await producer.start()
    
    logger.info("Sentiment Worker live. Waiting for posts...")

    try:
        async for msg in consumer:
            data = json.loads(msg.value)
            text = data.get("title", "") + " " + data.get("text", "")
            
            # NLP Inference
            result = sentiment_task(text[:512])[0] # Roberta max length is 512
            
            # Map labels to scores
            # "positive" -> 1.0, "neutral" -> 0.0, "negative" -> -1.0
            score = 0.0
            if result['label'] == 'positive': score = result['score']
            elif result['label'] == 'negative': score = -result['score']
            
            enriched_payload = {
                **data,
                "sentiment_score": round(score, 4),
                "sentiment_label": result['label']
            }
            
            await producer.send("social_enriched", json.dumps(enriched_payload).encode('utf-8'))
            logger.debug(f"Scored {data.get('subreddit')}: {score}")
            
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(process_sentiment())