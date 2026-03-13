from __future__ import annotations

from backup_pilot.encryption.fernet_encryptor import FernetEncryptor
from backup_pilot.encryption.none_encryptor import NoOpEncryptor

_ENCRYPTION_KEY_ENV = "BACKUP_PILOT_ENCRYPTION_KEY"


def create_encryptor(name: str, key: str | None = None):
    if name == "none":
        return NoOpEncryptor()
    if name == "fernet":
        effective_key = key
        if not effective_key:
            raise ValueError(
                f"Encryption is 'fernet' but {_ENCRYPTION_KEY_ENV} is not set. "
                "Set the environment variable to a base64-encoded Fernet key."
            )
        return FernetEncryptor(effective_key)
    raise ValueError(f"Unsupported encryption: {name}")
