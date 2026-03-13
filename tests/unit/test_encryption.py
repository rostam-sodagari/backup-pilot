from __future__ import annotations

from io import BytesIO

import pytest

from backup_pilot.encryption import FernetEncryptor, NoOpEncryptor, create_encryptor
from cryptography.fernet import Fernet


def test_noop_encryptor_round_trip():
    data = b"hello backup data"
    encryptor = NoOpEncryptor()
    encrypted = encryptor.encrypt(BytesIO(data))
    decrypted = encryptor.decrypt(encrypted)
    assert decrypted.read() == data


def test_fernet_encryptor_round_trip():
    key = Fernet.generate_key()
    encryptor = FernetEncryptor(key)
    data = b"secret backup content"
    encrypted_stream = encryptor.encrypt(BytesIO(data))
    encrypted_bytes = encrypted_stream.read()
    assert encrypted_bytes != data
    decrypted = encryptor.decrypt(BytesIO(encrypted_bytes))
    assert decrypted.read() == data


def test_fernet_encryptor_accepts_str_key():
    key = Fernet.generate_key().decode("ascii")
    encryptor = FernetEncryptor(key)
    data = b"test"
    decrypted = encryptor.decrypt(encryptor.encrypt(BytesIO(data)))
    assert decrypted.read() == data


def test_create_encryptor_none():
    enc = create_encryptor("none")
    assert isinstance(enc, NoOpEncryptor)


def test_create_encryptor_fernet_with_key():
    key = Fernet.generate_key().decode("ascii")
    enc = create_encryptor("fernet", key=key)
    assert isinstance(enc, FernetEncryptor)


def test_create_encryptor_fernet_without_key_raises():
    with pytest.raises(ValueError, match="BACKUP_PILOT_ENCRYPTION_KEY"):
        create_encryptor("fernet", key=None)


def test_create_encryptor_unsupported_raises():
    with pytest.raises(ValueError, match="Unsupported encryption"):
        create_encryptor("aes256")
