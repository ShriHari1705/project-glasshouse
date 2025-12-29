import feedparser
import asyncio
import json
import time
from aiokafka import AIOKafkaProducer
from loguru import logger

RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://cryptoslate.com/feed/"
]

async def poll_rss():
    producer = AIOKafkaProducer(bootstrap_servers='redpanda:9092')
    await producer.start()
    
    seen_links = set() # Simple deduplication
    
    logger.info(f"RSS Producer live. Polling {len(RSS_FEEDS)} feeds.")

    while True:
        for url in RSS_FEEDS:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries:
                    if entry.link not in seen_links:
                        payload = {
                            "title": entry.title,
                            "summary": entry.get("summary", ""),
                            "link": entry.link,
                            "published": entry.get("published", ""),
                            "source": url
                        }
                        await producer.send_and_wait(
                            "social.sentiment.raw", 
                            json.dumps(payload).encode('utf-8')
                        )
                        seen_links.add(entry.link)
                        logger.info(f"New RSS entry: {entry.title[:50]}...")
            except Exception as e:
                logger.error(f"RSS Error from {url}: {e}")
        
        # Keep set size manageable
        if len(seen_links) > 1000: seen_links.clear()
        
        # Heartbeat for Health Check
        with open("/tmp/heartbeat_social", "w") as f:
            f.write(str(time.time()))
            
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(poll_rss())