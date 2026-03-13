# BackupPilot

![BackupPilot screenshot](screenshot.png)

BackupPilot is a cross-platform Python command-line utility for backing up and restoring multiple databases with pluggable storage backends (local filesystem, AWS S3, Google Cloud Storage, Azure Blob Storage), compression, and optional notifications.

## Features

- **Multiple databases**: MySQL, PostgreSQL, MongoDB, SQLite (extensible to others).
- **Backup types**: Full, with design for incremental and differential backups where supported.
- **Storage options**: Local filesystem, AWS S3, Google Cloud Storage, Azure Blob Storage.
- **Compression**: Gzip by default, with an extensible compression interface.
- **Encryption**: Optional at-rest encryption for backups; use `encryption: none` (default) or `encryption: fernet` with the key supplied via the `BACKUP_PILOT_ENCRYPTION_KEY` environment variable (base64-encoded Fernet key).
- **Backup rotation**: Optional retention per profile (`retention_count` and/or `retention_days`); run `backup-pilot rotate` to delete old backups.
- **Logging**: Config-driven log level, optional file output, and JSON format (see `logging` in config).
- **Notifications**: Slack and email notifiers with error handling so backup/restore never fail due to notification delivery.
- **Restore operations**: Restore from backup artifacts, with selective restore where supported.
- **Backup history**: Per-config backup history tracking with a `list-backups` command showing backup metadata.

## Installation

```bash
pip install .
```

This will install the `backup-pilot` executable.

## Quickstart

Show CLI help:

```bash
backup-pilot --help
```

### Example configuration

Create a `backup_pilot.yaml` file in your working directory (or use one of the example configs under `examples/`):

```yaml
databases:
  local_mysql:
    type: mysql
    host: localhost
    port: 3306
    username: root
    password: example
    database: app_db

storage:
  local_fs:
    type: local
    options:
      root_dir: ./backups

backups:
  daily_mysql_full:
    database: local_mysql
    storage: local_fs
    backup_type: full
    compression: gzip
    # encryption: none   # default; use "fernet" and set BACKUP_PILOT_ENCRYPTION_KEY for encrypted backups
    # retention_count: 7   # keep at most 7 backups per profile
    # retention_days: 30   # delete backups older than 30 days

# logging:
#   level: INFO
#   file: /var/log/backup_pilot.log
#   json: false

notifications:
  slack:
    webhook_url: "https://hooks.slack.com/services/XXX/YYY/ZZZ"
```

Run a backup using the profile:

```bash
backup-pilot backup --profile daily_mysql_full --config-file backup_pilot.yaml
```

Each successful backup is recorded in a history file that lives next to your config file:

- For `backup_pilot.yaml` the history file is `backup_pilot.history.jsonl`.
- Each line is a JSON record containing the backup ID, profile, database type/name, storage location, timestamps, and (where available) size in bytes.

You can list recorded backups:

```bash
backup-pilot list-backups --config-file backup_pilot.yaml
```

Filter by profile and limit the number of results:

```bash
backup-pilot list-backups --config-file backup_pilot.yaml --profile daily_mysql_full --limit 10
```

Restore from a backup:

```bash
backup-pilot restore --profile daily_mysql_full --backup-id 20250101010101 --config-file backup_pilot.yaml
```

List configured profiles:

```bash
backup-pilot list-configs --config-file backup_pilot.yaml
```

Test a database connection:

```bash
backup-pilot test-connection --db-profile local_mysql --config-file backup_pilot.yaml
```

Run retention (rotate old backups according to `retention_count` / `retention_days`):

```bash
backup-pilot rotate --config-file backup_pilot.yaml
backup-pilot rotate --config-file backup_pilot.yaml --profile daily_mysql_full
```

The CLI currently exposes the following top-level commands:

- `backup-pilot backup`
- `backup-pilot restore`
- `backup-pilot rotate`
- `backup-pilot test-connection`
- `backup-pilot list-configs`
- `backup-pilot list-backups`

## Docker

Build the image:

```bash
docker build -t backup-pilot .
```

Run a backup (mount config and backup directory; set env vars for credentials/keys as needed):

```bash
docker run --rm -v /path/to/backup_pilot.yaml:/config/backup_pilot.yaml -v /path/to/backups:/backups backup-pilot backup --profile daily_mysql_full --config-file /config/backup_pilot.yaml
```

Use environment variables for secrets (e.g. `BACKUP_PILOT_ENCRYPTION_KEY`, `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` for S3, or your cloud provider’s preferred vars).

## CI

GitHub Actions workflow (`.github/workflows/ci.yml`) runs on push and pull requests: tests (Python 3.10–3.12), Ruff and Black lint, and package build.

## Examples

- **Basic local backup config**: `examples/config-basic.yaml`
- **AWS S3 backup config**: `examples/config-aws.yaml`
- **Google Cloud Storage backup config**: `examples/config-gcp.yaml`
- **Azure Blob Storage backup config**: `examples/config-azure.yaml`
- **Cron script example**: `examples/cron/backup-mysql-daily`

These examples are starting points; you should adapt hostnames, credentials, buckets/containers, and schedule to your environment.

