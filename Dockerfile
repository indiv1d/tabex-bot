FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    BOT_DB_PATH=/app/data/tabex.db

WORKDIR /app

RUN useradd --create-home --shell /bin/bash appuser

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

RUN mkdir -p /app/data && chown -R appuser:appuser /app

USER appuser

CMD ["python", "src/main.py"]
