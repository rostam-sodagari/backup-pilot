FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      default-mysql-client \
      postgresql-client \
      curl \
      ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    curl -fsSL https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.14.1.tgz -o /tmp/mongodb-tools.tgz && \
    cd /tmp && tar -xzf mongodb-tools.tgz && \
    mv /tmp/mongodb-database-tools-*/bin/* /usr/local/bin/ && \
    rm -rf /tmp/mongodb-database-tools-* /tmp/mongodb-tools.tgz

COPY pyproject.toml ./
COPY backup_pilot/ ./backup_pilot/

RUN pip install --no-cache-dir .

ENTRYPOINT ["backup-pilot"]
