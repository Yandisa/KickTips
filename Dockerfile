FROM python:3.13-slim

WORKDIR /app

# System deps for psycopg2 + lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Entrypoint: migrates + starts gunicorn
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
