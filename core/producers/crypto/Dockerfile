# --- Stage 1: Builder ---
FROM python:3.11-slim as builder

WORKDIR /app

# Install system dependencies for C-extensions (if any)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements to leverage Docker cache
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# --- Stage 2: Final Runtime ---
FROM python:3.11-slim

WORKDIR /app

# Copy only the installed packages from the builder stage
COPY --from=builder /root/.local /root/.local
# Copy the source code
COPY . .

# Ensure scripts can find the installed packages
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1

# Create the heartbeat file to avoid healthcheck failure on start
RUN touch /tmp/heartbeat && touch /tmp/heartbeat_social

# Run the producer
CMD ["python", "core/producers/crypto/main.py"]