# --- Stage 1: Dependency Builder ---
FROM python:3.11-slim as builder

WORKDIR /app

# Install git if needed for specific HF models, and build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# Change --index-url to --extra-index-url to keep PyPI visible
RUN pip install --user --no-cache-dir \
    torch==2.5.1+cpu \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    transformers==4.47.1 \
    aiokafka==0.12.0 \
    loguru==0.7.2

# --- Stage 2: Model Downloader (The "Pro" Step) ---
# We download the model weights now so they are baked into the image layer
RUN python -c "from transformers import pipeline; \
    pipeline('sentiment-analysis', \
    model='cardiffnlp/twitter-roberta-base-sentiment-latest', \
    tokenizer='cardiffnlp/twitter-roberta-base-sentiment-latest')"

# --- Stage 3: Final Runtime ---
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages and the cached model from builder
COPY --from=builder /root/.local /root/.local
COPY --from=builder /root/.cache/huggingface /root/.cache/huggingface

# Copy source code
COPY core/processors/sentiment/worker.py .

ENV PATH=/root/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1
ENV TRANSFORMERS_OFFLINE=1 

# Start the worker
CMD ["python", "worker.py"]