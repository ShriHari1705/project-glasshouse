import json
import asyncio
import clickhouse_connect
from aiokafka import AIOKafkaConsumer
from transformers import pipeline
from loguru import logger
from datetime import datetime
import dateutil.parser  # Ensure 'python-dateutil' is in your requirements.txt

# Configuration
INPUT_TOPIC = "social.sentiment.raw"
KAFKA_BROKER = "redpanda:9092"
MODEL_NAME = "/app/model_weights"

# ClickHouse Credentials
CH_HOST = "clickhouse"
CH_USER = "admin"
CH_PASS = "password"
CH_DB = "glasshouse"

def parse_date(date_str):
    """Convert RSS date strings to ISO format for ClickHouse DateTime64."""
    try:
        # Handles most RSS formats (RFC 2822, ISO, etc.)
        return dateutil.parser.parse(date_str)
    except Exception:
        return datetime.utcnow()

async def process_sentiment():
    # 1. Initialize ClickHouse Client
    try:
        client = clickhouse_connect.get_client(
            host=CH_HOST, 
            username=CH_USER, 
            password=CH_PASS, 
            database=CH_DB
        )
        logger.info("Successfully connected to ClickHouse.")
    except Exception as e:
        logger.error(f"ClickHouse Connection Failed: {e}")
        return

    # 2. Initialize the AI "Brain"
    # Note: On CPU, this takes 1-3 minutes to load from disk!
    logger.info(f"Loading RoBERTa model from {MODEL_NAME}...")
    sentiment_task = pipeline("sentiment-analysis", model=MODEL_NAME, device=-1)
    logger.info("Model loaded. Worker is now live and listening to Kafka.")

    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="sentiment_group",
        session_timeout_ms=60000,
        max_poll_interval_ms=300000,
        max_poll_records=5,
        auto_offset_reset="earliest"
    )

    await consumer.start()

    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            
            # Extract data for processing
            headline_text = data.get('title', '')
            summary_text = data.get('summary', '')
            text_to_score = f"{headline_text} {summary_text}".strip()

            if not text_to_score:
                continue

            # 3. Run AI Inference
            result = sentiment_task(text_to_score[:512])[0] # Truncate to model limit
            
            # Map RoBERTa labels to scores
            label_map = {"LABEL_0": 0.0, "LABEL_1": 0.5, "LABEL_2": 1.0}
            score = float(label_map.get(result['label'], 0.5))

            # 4. Build row matching ClickHouse 'DESCRIBE' exactly:
            # Columns: source, timestamp, headline, sentiment_score, sentiment_label, link, raw_json
            row = [
                "RSS_FEED",                         # source
                parse_date(data.get("published")),  # timestamp (DateTime64)
                headline_text,                      # headline
                score,                              # sentiment_score
                result['label'],                    # sentiment_label
                data.get("link", ""),               # link
                json.dumps(data)                    # raw_json
            ]

            # 5. Direct Insert using the verified column names
            client.insert(
                'social_enriched', 
                [row], 
                column_names=[
                    'source', 'timestamp', 'headline', 
                    'sentiment_score', 'sentiment_label', 
                    'link', 'raw_json'
                ]
            )
            
            logger.info(f"✅ Scored & Saved: {headline_text[:40]}... [{result['label']}]")

    except Exception as e:
        logger.error(f"Worker execution error: {e}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(process_sentiment())