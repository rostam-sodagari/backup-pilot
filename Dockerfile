FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml ./
COPY backup_pilot/ ./backup_pilot/

RUN pip install --no-cache-dir .

ENTRYPOINT ["backup-pilot"]
