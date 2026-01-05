FROM python:3.11-slim

WORKDIR /app

# Install system tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 1. Copy requirements from the ROOT (context is .)
COPY requirements.txt .
RUN pip install --no-cache-dir \
    torch==2.5.1 --extra-index-url https://download.pytorch.org/whl/cpu \
    && pip install --no-cache-dir -r requirements.txt

# 2. Copy the worker and weights from their specific sub-folder
COPY ./core/processors/sentiment/worker.py .
#COPY ./core/processors/sentiment/model_weights/ ./model_weights/

# 3. Environment to ensure no outside calls
ENV TRANSFORMERS_OFFLINE=1

CMD ["python", "worker.py"]