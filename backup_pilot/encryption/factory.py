from __future__ import annotations

from backup_pilot.encryption.fernet_encryptor import FernetEncryptor
from backup_pilot.encryption.none_encryptor import NoOpEncryptor

_ENCRYPTION_KEY_ENV = "BACKUP_PILOT_ENCRYPTION_KEY"


class EncryptionConfigurationError(ValueError):
    """
    Raised when encryption configuration is invalid or incomplete.

    This is intended to surface clear, user-facing error messages and can be
    logged or included in notifications without exposing internal details.
    """


def create_encryptor(name: str, key: str | None = None):
    if name == "none":
        return NoOpEncryptor()
    if name == "fernet":
        effective_key = key
        if not effective_key:
            raise EncryptionConfigurationError(
                f"Encryption is configured as 'fernet' but {_ENCRYPTION_KEY_ENV} is not set "
                "or is empty. Set this environment variable (for example via a .env file) "
                "to a base64-encoded Fernet key."
            )
        return FernetEncryptor(effective_key)
    raise EncryptionConfigurationError(f"Unsupported encryption: {name}")
