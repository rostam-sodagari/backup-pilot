# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-03-13

### Added

- Initial release of the `backup-pilot` CLI.
- Support for backing up and restoring multiple databases: MySQL, PostgreSQL, MongoDB, and SQLite.
- Pluggable storage backends: local filesystem, AWS S3, Google Cloud Storage, and Azure Blob Storage.
- Gzip compression with an extensible compression interface.
- Optional at-rest encryption using Fernet (configured via the `BACKUP_PILOT_ENCRYPTION_KEY` environment variable).
- Backup rotation with `retention_count` and `retention_days`, and a `rotate` command to delete old backups.
- Config-driven logging with optional file output and JSON log format.
- Notifications via Slack and email, with failures not causing backup/restore to fail.
- Restore operations from backup artifacts, with selective restore where supported.
- Per-config backup history tracking with a `list-backups` command.
- Docker image and basic CI workflow for testing, linting, and building the package.

