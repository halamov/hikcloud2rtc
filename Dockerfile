FROM python:3.12-slim

# ffmpeg (decode/remux для /frame) + ca-certificates
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py /app/app.py
COPY cloudcam/ /app/cloudcam/
RUN mkdir -p /app/data

# 8000 — python API: /health /cameras /snapshot /frame /stream.ts /keys/*
EXPOSE 8000
CMD ["python", "/app/app.py"]
