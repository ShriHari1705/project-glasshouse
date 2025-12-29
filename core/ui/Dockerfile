FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir streamlit clickhouse-connect pandas

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]