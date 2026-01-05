FROM python:3.11-slim

WORKDIR /app

# Copy requirements from the root
COPY requirements.txt .

# Resilient installation: high timeout and cache disabled
RUN pip install --no-cache-dir --default-timeout=1000 -r requirements.txt

# Copy the script
COPY ./core/producers/social/rss_producer.py .

CMD ["python", "rss_producer.py"]