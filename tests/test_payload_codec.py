"""Tests for the encrypting Temporal payload codec.

Regression coverage for the audit finding "no DataConverter, no PayloadCodec":
every workflow input, result, signal argument and activity argument was written
into Temporal history as plaintext JSON on the shared Temporal RDS instance, and
"no PHI in workflow history" was enforced by docstring convention only.

What these tests prove:
  - A real round trip: encode then decode returns byte-identical payload data
    AND byte-identical original metadata (the original encoding survives).
  - Ciphertext does not contain the plaintext, and the emitted payload is
    stamped with the encrypted encoding, the key id, and the algorithm.
  - FAIL CLOSED in production mode: with no key configured, building the codec
    raises instead of producing a codec that would pass plaintext through.
  - The plaintext no-op path requires the explicit flag, that flag defaults OFF,
    and it is REFUSED when ENVIRONMENT names production.
  - decode() raises on an encrypted payload it cannot decrypt — no key, unknown
    key id, tampered ciphertext, unsupported algorithm, truncated data.
  - decode() DOES pass through payloads with no encrypted marker, because
    Adaptix already has plaintext histories written before this codec shipped
    and they must stay replayable.
  - Rotation works: a payload encrypted under a retired key still decodes while
    that key remains in the keyring.
  - The data converter keeps the SDK default payload/failure converters and
    replaces only the codec slot.

What these tests do NOT prove:
  - That a live Temporal server stores the ciphertext (that needs a deployed
    worker and a history dump).
  - AES-GCM correctness itself — that is the `cryptography` library's job.
  - That the production key is provisioned in AWS Secrets Manager.
"""

from __future__ import annotations

import base64
import json

import pytest
from temporalio.api.common.v1 import Payload

from temporal_app import config
from temporal_app.codec import (
    ALGORITHM,
    ENCRYPTED_ENCODING,
    METADATA_ALGORITHM,
    METADATA_KEY_ID,
    NONCE_LENGTH_BYTES,
    EncryptionPayloadCodec,
    Keyring,
    PayloadCodecError,
    build_data_converter,
)

# Fixed NON-SECRET test keys. Published in the repo on purpose so nobody can
# mistake them for production key material.
_KEY_A = base64.b64encode(b"adaptix-test-key-a-not-a-secret!").decode()
_KEY_B = base64.b64encode(b"adaptix-test-key-b-not-a-secret!").decode()

_PLAINTEXT = b'{"claim_id":"11111111-2222-3333-4444-555555555555"}'


def _keyring(primary: str = "test-a", **extra: str) -> Keyring:
    keys = {"test-a": _KEY_A}
    keys.update(extra)
    return Keyring.from_secret_value(
        json.dumps({"primary_key_id": primary, "keys": keys})
    )


def _payload(data: bytes = _PLAINTEXT) -> Payload:
    return Payload(metadata={"encoding": b"json/plain"}, data=data)


def _clear_codec_env(monkeypatch) -> None:
    """Remove every codec-related variable so a test starts from a clean slate."""
    monkeypatch.delenv(config.PAYLOAD_CODEC_KEY_ENV, raising=False)
    monkeypatch.delenv(config.PAYLOAD_CODEC_PLAINTEXT_ENV, raising=False)
    monkeypatch.delenv(config.ENVIRONMENT_ENV, raising=False)


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_encode_decode_round_trip_returns_the_original_payload():
    """Decrypting an encrypted payload yields exactly what was encrypted."""
    codec = EncryptionPayloadCodec(_keyring())

    encoded = await codec.encode([_payload()])
    decoded = await codec.decode(encoded)

    assert len(decoded) == 1
    assert decoded[0].data == _PLAINTEXT
    # The ORIGINAL encoding metadata is restored, not just the bytes.
    assert decoded[0].metadata["encoding"] == b"json/plain"


@pytest.mark.asyncio
async def test_round_trip_preserves_every_payload_in_order():
    """Multi-payload calls (activity args) keep count and order."""
    codec = EncryptionPayloadCodec(_keyring())
    originals = [_payload(f'{{"n":{i}}}'.encode()) for i in range(5)]

    decoded = await codec.decode(await codec.encode(originals))

    assert [p.data for p in decoded] == [p.data for p in originals]


@pytest.mark.asyncio
async def test_encoded_payload_hides_plaintext_and_is_stamped():
    """The emitted payload is ciphertext and carries key id + algorithm."""
    codec = EncryptionPayloadCodec(_keyring())

    (encoded,) = await codec.encode([_payload()])

    assert _PLAINTEXT not in encoded.data
    assert b"claim_id" not in encoded.data
    assert encoded.metadata["encoding"] == ENCRYPTED_ENCODING
    assert encoded.metadata[METADATA_KEY_ID] == b"test-a"
    assert encoded.metadata[METADATA_ALGORITHM] == ALGORITHM
    # nonce is prepended, so the payload is strictly longer than the nonce.
    assert len(encoded.data) > NONCE_LENGTH_BYTES


@pytest.mark.asyncio
async def test_two_encodes_of_the_same_value_differ():
    """A fresh nonce per payload means identical inputs are not identical on the wire."""
    codec = EncryptionPayloadCodec(_keyring())

    (first,) = await codec.encode([_payload()])
    (second,) = await codec.encode([_payload()])

    assert first.data != second.data


# ---------------------------------------------------------------------------
# Fail closed: no key in production mode
# ---------------------------------------------------------------------------


def test_from_environment_fails_closed_without_a_key(monkeypatch):
    """No key and no explicit local flag: refuse to build a codec at all."""
    _clear_codec_env(monkeypatch)
    monkeypatch.setenv(config.ENVIRONMENT_ENV, "production")

    with pytest.raises(PayloadCodecError) as exc:
        EncryptionPayloadCodec.from_environment()

    assert config.PAYLOAD_CODEC_KEY_ENV in str(exc.value)


def test_from_environment_fails_closed_without_a_key_when_env_is_unset(monkeypatch):
    """Fail-closed does not depend on ENVIRONMENT being set to production."""
    _clear_codec_env(monkeypatch)

    with pytest.raises(PayloadCodecError):
        EncryptionPayloadCodec.from_environment()


def test_build_data_converter_propagates_the_failure(monkeypatch):
    """The wiring helper does not swallow a missing key into a plaintext codec."""
    _clear_codec_env(monkeypatch)

    with pytest.raises(PayloadCodecError):
        build_data_converter()


def test_codec_cannot_be_constructed_keyless_without_explicit_passthrough():
    """The constructor itself refuses a keyless codec unless passthrough is asked for."""
    with pytest.raises(PayloadCodecError):
        EncryptionPayloadCodec(None)


@pytest.mark.parametrize(
    ("secret", "label"),
    [
        ("", "empty"),
        ("   ", "whitespace"),
        ("not-base64!!", "not base64"),
        (base64.b64encode(b"too-short").decode(), "wrong key length"),
        ('{"keys": {}}', "empty key set"),
        ('{"keys": {"k": "' + _KEY_A + '"}}', "no primary_key_id"),
        (
            '{"primary_key_id": "missing", "keys": {"k": "' + _KEY_A + '"}}',
            "primary absent",
        ),
        ("{not json", "malformed json"),
    ],
)
def test_malformed_secret_is_refused(secret: str, label: str):
    """A malformed keyring stops the worker instead of degrading it."""
    with pytest.raises(PayloadCodecError):
        Keyring.from_secret_value(secret)


def test_error_messages_never_contain_key_material():
    """A rejected key's VALUE is never echoed into an error string."""
    bad = base64.b64encode(b"short-key-value").decode()
    with pytest.raises(PayloadCodecError) as exc:
        Keyring.from_secret_value(bad)

    assert bad not in str(exc.value)
    assert "short-key-value" not in str(exc.value)


# ---------------------------------------------------------------------------
# Plaintext no-op mode: explicit flag only, never in production
# ---------------------------------------------------------------------------


def test_plaintext_mode_is_off_by_default(monkeypatch):
    """Without the flag there is no no-op mode — the codec refuses to exist."""
    _clear_codec_env(monkeypatch)

    with pytest.raises(PayloadCodecError):
        EncryptionPayloadCodec.from_environment()


@pytest.mark.parametrize("flag", ["", "0", "false", "off", "no", "maybe", "FALSE"])
def test_plaintext_mode_requires_an_affirmative_flag(monkeypatch, flag: str):
    """Only an explicit affirmative enables no-op mode; a typo does not."""
    _clear_codec_env(monkeypatch)
    monkeypatch.setenv(config.PAYLOAD_CODEC_PLAINTEXT_ENV, flag)

    with pytest.raises(PayloadCodecError):
        EncryptionPayloadCodec.from_environment()


@pytest.mark.parametrize("flag", ["1", "true", "TRUE", "yes", "on"])
def test_plaintext_mode_enabled_by_explicit_flag_outside_production(
    monkeypatch, flag: str
):
    """With the explicit flag and a non-production environment, encode passes through."""
    _clear_codec_env(monkeypatch)
    monkeypatch.setenv(config.PAYLOAD_CODEC_PLAINTEXT_ENV, flag)
    monkeypatch.setenv(config.ENVIRONMENT_ENV, "local")

    codec = EncryptionPayloadCodec.from_environment()

    assert codec.is_encrypting is False
    assert codec.primary_key_id is None


@pytest.mark.parametrize("environment", ["production", "PRODUCTION", " prod ", "Prod"])
def test_plaintext_mode_is_refused_in_production(monkeypatch, environment: str):
    """The local escape hatch is refused outright in a production environment."""
    _clear_codec_env(monkeypatch)
    monkeypatch.setenv(config.PAYLOAD_CODEC_PLAINTEXT_ENV, "true")
    monkeypatch.setenv(config.ENVIRONMENT_ENV, environment)

    with pytest.raises(PayloadCodecError) as exc:
        EncryptionPayloadCodec.from_environment()

    assert config.PAYLOAD_CODEC_PLAINTEXT_ENV in str(exc.value)


def test_a_configured_key_wins_over_the_plaintext_flag(monkeypatch):
    """A present key always encrypts, even if someone also set the local flag."""
    _clear_codec_env(monkeypatch)
    monkeypatch.setenv(config.PAYLOAD_CODEC_PLAINTEXT_ENV, "true")
    monkeypatch.setenv(config.ENVIRONMENT_ENV, "staging")
    monkeypatch.setenv(config.PAYLOAD_CODEC_KEY_ENV, _KEY_A)

    codec = EncryptionPayloadCodec.from_environment()

    assert codec.is_encrypting is True


@pytest.mark.asyncio
async def test_plaintext_mode_still_refuses_to_read_encrypted_history():
    """No-op mode is not a decryption bypass: encrypted payloads still raise."""
    encrypting = EncryptionPayloadCodec(_keyring())
    encrypted = await encrypting.encode([_payload()])

    passthrough = EncryptionPayloadCodec(None, plaintext_passthrough=True)

    assert await passthrough.encode([_payload()]) == [_payload()]
    with pytest.raises(PayloadCodecError):
        await passthrough.decode(encrypted)


# ---------------------------------------------------------------------------
# decode(): raise on undecryptable, pass through unmarked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_decode_passes_through_payloads_written_before_this_codec():
    """Plaintext history predating the codec must stay replayable."""
    codec = EncryptionPayloadCodec(_keyring())
    legacy = _payload(b'{"era_file_path":"s3://bucket/key"}')

    (decoded,) = await codec.decode([legacy])

    assert decoded.data == legacy.data
    assert decoded.metadata["encoding"] == b"json/plain"


@pytest.mark.asyncio
async def test_decode_raises_on_unknown_key_id():
    """A payload encrypted with a key this worker does not hold is not guessed at."""
    encoded = await EncryptionPayloadCodec(_keyring()).encode([_payload()])

    other = EncryptionPayloadCodec(
        Keyring.from_secret_value(
            json.dumps({"primary_key_id": "test-b", "keys": {"test-b": _KEY_B}})
        )
    )

    with pytest.raises(PayloadCodecError) as exc:
        await other.decode(encoded)

    assert "test-a" in str(exc.value)


@pytest.mark.asyncio
async def test_decode_raises_on_tampered_ciphertext():
    """AES-GCM authentication failure surfaces as an error, never as garbage."""
    codec = EncryptionPayloadCodec(_keyring())
    (encoded,) = await codec.encode([_payload()])

    tampered = Payload(
        metadata=dict(encoded.metadata),
        data=encoded.data[:-1] + bytes([encoded.data[-1] ^ 0xFF]),
    )

    with pytest.raises(PayloadCodecError) as exc:
        await codec.decode([tampered])

    assert "authentication tag" in str(exc.value)


@pytest.mark.asyncio
async def test_decode_raises_on_wrong_key_for_a_known_key_id():
    """Same key id, different key bytes: the tag check catches it."""
    (encoded,) = await EncryptionPayloadCodec(_keyring()).encode([_payload()])

    impostor = EncryptionPayloadCodec(
        Keyring.from_secret_value(
            json.dumps({"primary_key_id": "test-a", "keys": {"test-a": _KEY_B}})
        )
    )

    with pytest.raises(PayloadCodecError):
        await impostor.decode([encoded])


@pytest.mark.asyncio
async def test_decode_raises_on_unsupported_algorithm():
    """An algorithm this worker does not implement is refused, not attempted."""
    codec = EncryptionPayloadCodec(_keyring())
    (encoded,) = await codec.encode([_payload()])
    encoded.metadata[METADATA_ALGORITHM] = b"AES-128-CBC"

    with pytest.raises(PayloadCodecError) as exc:
        await codec.decode([encoded])

    assert "unsupported algorithm" in str(exc.value)


@pytest.mark.asyncio
async def test_decode_raises_on_truncated_payload():
    """Data too short to hold a nonce plus ciphertext is an error, not a crash."""
    codec = EncryptionPayloadCodec(_keyring())

    truncated = Payload(
        metadata={
            "encoding": ENCRYPTED_ENCODING,
            METADATA_KEY_ID: b"test-a",
            METADATA_ALGORITHM: ALGORITHM,
        },
        data=b"short",
    )

    with pytest.raises(PayloadCodecError) as exc:
        await codec.decode([truncated])

    assert "truncated" in str(exc.value)


# ---------------------------------------------------------------------------
# Rotation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_retired_key_still_decodes_history_it_encrypted():
    """Rotation does not break open workflows encrypted under the previous key."""
    old = EncryptionPayloadCodec(_keyring(primary="test-a"))
    (encoded_under_old,) = await old.encode([_payload()])

    # Rotate: new primary, old key retained for decryption.
    rotated = EncryptionPayloadCodec(_keyring(primary="test-b", **{"test-b": _KEY_B}))

    (decoded,) = await rotated.decode([encoded_under_old])
    assert decoded.data == _PLAINTEXT

    # New writes use the new key.
    (fresh,) = await rotated.encode([_payload()])
    assert fresh.metadata[METADATA_KEY_ID] == b"test-b"


def test_bare_base64_key_derives_a_stable_key_id():
    """The local-dev bare-key form still stamps a stable, non-secret key id."""
    first = Keyring.from_secret_value(_KEY_A)
    second = Keyring.from_secret_value(_KEY_A)

    assert first.primary_key_id == second.primary_key_id
    assert first.primary_key_id not in _KEY_A


# ---------------------------------------------------------------------------
# Data converter wiring
# ---------------------------------------------------------------------------


def test_data_converter_replaces_only_the_codec_slot():
    """SDK payload/failure conversion is untouched; only the codec is added."""
    from temporalio.converter import DataConverter

    converter = build_data_converter(EncryptionPayloadCodec(_keyring()))

    assert isinstance(converter.payload_codec, EncryptionPayloadCodec)
    assert (
        converter.payload_converter_class
        is DataConverter.default.payload_converter_class
    )
    assert (
        converter.failure_converter_class
        is DataConverter.default.failure_converter_class
    )
    # The SDK's own default must remain codec-free — replace() must not mutate it.
    assert DataConverter.default.payload_codec is None


@pytest.mark.asyncio
async def test_data_converter_encrypts_real_values_end_to_end():
    """A Python value converted through the full DataConverter comes back intact."""
    converter = build_data_converter(EncryptionPayloadCodec(_keyring()))
    value = {"tenant_id": "tenant-abc", "amount_cents": 12345}

    payloads = await converter.encode([value])
    assert payloads[0].metadata["encoding"] == ENCRYPTED_ENCODING
    assert b"tenant-abc" not in payloads[0].data

    (decoded,) = await converter.decode(payloads)
    assert decoded == value
