FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DB_PATH=/data/recovery.db

WORKDIR /app

# Install dependencies (image is self-contained).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code.
COPY app ./app

# Initialize the SQLite database inside the image; the schema is also
# (re)created idempotently on every application startup.
RUN mkdir -p /data && python -c "from app.db import init_db; init_db()"

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
