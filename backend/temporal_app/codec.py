"""Encrypting Temporal ``PayloadCodec`` for the Adaptix worker fleet.

Every value a workflow accepts, returns, signals, queries, or passes to an
activity is serialised into a Temporal ``Payload`` and written into workflow
history on the Temporal server's Postgres database. Before this module existed,
those bytes were stored as plaintext JSON and "no PHI in workflow history" was a
docstring convention with nothing enforcing it.

This codec makes the boundary real: payload bytes are encrypted with
AES-256-GCM before they leave the worker process and decrypted only inside a
worker that holds the key. The Temporal server, its RDS instance, its backups,
and anyone reading history through the Temporal UI or ``tctl`` see ciphertext.

Wire format
-----------
``encode()`` replaces each inbound ``Payload`` with a NEW ``Payload``:

    metadata:
        encoding             = b"binary/encrypted"
        encryption-key-id    = <key id that encrypted this payload>
        encryption-algorithm = b"AES-256-GCM"
    data:
        <12-byte nonce> || <AES-GCM ciphertext of the original serialised Payload>

The ORIGINAL payload (its own metadata AND data) is serialised with
``Payload.SerializeToString()`` and encrypted whole, so the original encoding
metadata (``json/plain``, ``binary/null``, …) is itself hidden and restored
exactly on decode.

Fail-closed rules — read before changing anything here
------------------------------------------------------
1. ``encode()`` NEVER emits plaintext when a key is expected. With no usable
   key it raises :class:`PayloadCodecError`. A worker that cannot encrypt does
   not get to write to history "just this once".
2. ``decode()`` raises on any payload MARKED ``binary/encrypted`` that it
   cannot decrypt — unknown key id, missing key, or a failed GCM
   authentication tag (which means tampering or corruption).
3. ``decode()`` PASSES THROUGH payloads that carry no encrypted marker. That is
   not fail-open. Adaptix already has completed and in-flight workflow
   histories (claim submission, ERA posting, agency onboarding) written before
   this codec shipped; those payloads are plaintext and must stay readable or
   every one of them breaks on replay. Only decode is permissive, and only for
   payloads that were never encrypted in the first place.
4. Plaintext passthrough on ENCODE exists solely for local development and the
   test suite. It requires the explicit ``TEMPORAL_PAYLOAD_CODEC_PLAINTEXT_LOCAL``
   flag, that flag defaults OFF, and it is REFUSED outright when ``ENVIRONMENT``
   names a production environment.

Key material
------------
The key arrives the same way every other Adaptix worker secret arrives: AWS
Secrets Manager -> ECS task definition ``secrets`` block -> environment
variable. The worker reads ``TEMPORAL_PAYLOAD_CODEC_KEY`` and never calls
Secrets Manager itself, exactly as ``system_token_client`` reads
``CORE_PROVISIONING_TOKEN``.

Two secret shapes are accepted:

* A keyring JSON object — the production shape, because it supports rotation::

      {"primary_key_id": "2026-08", "keys": {"2026-08": "<base64 32 bytes>"}}

  ``primary_key_id`` names the key used to ENCRYPT. Every entry in ``keys`` can
  DECRYPT. Rotating means adding a new entry and repointing
  ``primary_key_id`` — the retired key stays in ``keys`` so open workflows and
  historical runs encrypted under it remain readable. Removing a key that any
  live history still uses is what breaks replay, so retire keys only after the
  runs that used them have closed.

* A bare base64-encoded 32-byte key — convenience for local development. Its
  key id is derived as a SHA-256 fingerprint of the key bytes, so it is stable
  but carries no rotation story.

Security
--------
* Key material is never logged, never returned, and never placed in an
  exception message. Errors name the key ID (an opaque label) and the failure
  mode only.
* Plaintext payload bytes are never logged. Payload contents may carry PHI;
  that is the entire reason this module exists.
* A fresh 12-byte nonce is generated per payload from ``os.urandom``. The codec
  runs on the worker's IO path, never inside the workflow sandbox, so
  non-deterministic randomness here is correct and does not affect replay.
"""

from __future__ import annotations

import base64
import binascii
import dataclasses
import hashlib
import json
import logging
import os
from dataclasses import dataclass

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from temporalio.api.common.v1 import Payload
from temporalio.converter import DataConverter, PayloadCodec

from temporal_app import config

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Wire-format constants
# --------------------------------------------------------------------------

#: ``encoding`` metadata value that marks a payload as produced by this codec.
ENCRYPTED_ENCODING: bytes = b"binary/encrypted"

#: Metadata key carrying the id of the key that encrypted the payload. Read on
#: decode to select the right key from the keyring during a rotation window.
METADATA_KEY_ID: str = "encryption-key-id"

#: Metadata key carrying the algorithm name, so a future algorithm change can
#: be detected rather than silently mis-decrypted.
METADATA_ALGORITHM: str = "encryption-algorithm"

#: The only algorithm this codec writes. AES-256-GCM is authenticated
#: encryption: a modified ciphertext fails the tag check instead of decrypting
#: to garbage.
ALGORITHM: bytes = b"AES-256-GCM"

#: AES-GCM key length in bytes (256-bit).
KEY_LENGTH_BYTES: int = 32

#: AES-GCM nonce length in bytes. 96-bit is the NIST-recommended GCM nonce size
#: and what ``AESGCM`` is optimised for.
NONCE_LENGTH_BYTES: int = 12


class PayloadCodecError(RuntimeError):
    """Raised when a payload cannot be encrypted or decrypted.

    Treated as fatal, not transient. Every cause — no key configured, an
    unknown key id, a failed authentication tag — is a deployment or integrity
    problem that retrying the same call cannot resolve.
    """


# --------------------------------------------------------------------------
# Keyring
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Keyring:
    """The set of AES-256 keys this worker can use, plus which one encrypts.

    ``keys`` maps key id -> 32 raw key bytes. ``primary_key_id`` must be a key
    in that mapping and is the one ``encode()`` uses. Every key in the mapping
    can decrypt, which is what makes rotation possible without breaking the
    histories written under a retired key.
    """

    primary_key_id: str
    keys: dict[str, bytes]

    def __post_init__(self) -> None:
        if not self.keys:
            raise PayloadCodecError("payload codec keyring contains no keys")
        if self.primary_key_id not in self.keys:
            raise PayloadCodecError(
                f"payload codec keyring primary_key_id '{self.primary_key_id}' "
                "is not present in its own key set"
            )

    @property
    def primary_key(self) -> bytes:
        return self.keys[self.primary_key_id]

    def key_for(self, key_id: str) -> bytes:
        """Return the key for ``key_id`` or raise if this worker does not hold it."""
        key = self.keys.get(key_id)
        if key is None:
            raise PayloadCodecError(
                f"payload was encrypted with key id '{key_id}', which this worker "
                "does not hold. Add that key to the payload codec secret "
                "(retired keys must stay in the keyring while any history that "
                "used them is still readable)."
            )
        return key

    @staticmethod
    def _decode_key(key_id: str, encoded: object) -> bytes:
        """Base64-decode one key and enforce the AES-256 length.

        The key VALUE never appears in an error message — only its id.
        """
        if not isinstance(encoded, str) or not encoded.strip():
            raise PayloadCodecError(
                f"payload codec key '{key_id}' is not a base64 string"
            )
        try:
            raw = base64.b64decode(encoded.strip(), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise PayloadCodecError(
                f"payload codec key '{key_id}' is not valid base64"
            ) from exc
        if len(raw) != KEY_LENGTH_BYTES:
            raise PayloadCodecError(
                f"payload codec key '{key_id}' must decode to "
                f"{KEY_LENGTH_BYTES} bytes (AES-256); got {len(raw)}"
            )
        return raw

    @classmethod
    def from_secret_value(cls, secret_value: str) -> Keyring:
        """Build a keyring from the raw ``TEMPORAL_PAYLOAD_CODEC_KEY`` value.

        Accepts the production keyring JSON object or a bare base64 key (local
        development). Raises :class:`PayloadCodecError` on anything else — a
        malformed secret must stop the worker, not silently degrade it.
        """
        value = (secret_value or "").strip()
        if not value:
            raise PayloadCodecError(
                f"{config.PAYLOAD_CODEC_KEY_ENV} is empty — no payload encryption "
                "key is configured."
            )

        if value.startswith("{"):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError as exc:
                raise PayloadCodecError(
                    f"{config.PAYLOAD_CODEC_KEY_ENV} starts with '{{' but is not "
                    "valid JSON. Expected "
                    '{"primary_key_id": "...", "keys": {"...": "<base64>"}}.'
                ) from exc
            if not isinstance(parsed, dict):
                raise PayloadCodecError(
                    f"{config.PAYLOAD_CODEC_KEY_ENV} JSON must be an object"
                )

            raw_keys = parsed.get("keys")
            if not isinstance(raw_keys, dict) or not raw_keys:
                raise PayloadCodecError(
                    f"{config.PAYLOAD_CODEC_KEY_ENV} JSON must carry a non-empty "
                    "'keys' object mapping key id -> base64 key"
                )
            primary = parsed.get("primary_key_id")
            if not isinstance(primary, str) or not primary.strip():
                raise PayloadCodecError(
                    f"{config.PAYLOAD_CODEC_KEY_ENV} JSON must carry a "
                    "'primary_key_id' string"
                )

            keys = {
                str(key_id): cls._decode_key(str(key_id), encoded)
                for key_id, encoded in raw_keys.items()
            }
            return cls(primary_key_id=primary.strip(), keys=keys)

        # Bare base64 key. Derive a stable, non-secret id from the key bytes so
        # payload metadata still names the key that encrypted it.
        raw = cls._decode_key("derived", value)
        key_id = hashlib.sha256(raw).hexdigest()[:16]
        return cls(primary_key_id=key_id, keys={key_id: raw})


# --------------------------------------------------------------------------
# Codec
# --------------------------------------------------------------------------


class EncryptionPayloadCodec(PayloadCodec):
    """AES-256-GCM ``PayloadCodec`` for Adaptix Temporal payloads.

    Construct with :meth:`from_environment` in worker startup paths. The
    explicit constructor exists for tests, which need to build a codec with a
    known keyring without touching process environment.
    """

    def __init__(
        self,
        keyring: Keyring | None,
        *,
        plaintext_passthrough: bool = False,
    ) -> None:
        if keyring is None and not plaintext_passthrough:
            raise PayloadCodecError(
                "EncryptionPayloadCodec requires a keyring unless plaintext "
                "passthrough is explicitly enabled for local development."
            )
        self._keyring = keyring
        self._plaintext_passthrough = plaintext_passthrough

    # -- construction ------------------------------------------------------ #
    @classmethod
    def from_environment(cls) -> EncryptionPayloadCodec:
        """Build the codec from process environment, failing closed.

        Reads the environment live (rather than import-time constants) so a
        worker started after its ECS secret injection, and a test using
        ``monkeypatch.setenv``, both observe the same values.

        Raises :class:`PayloadCodecError` when:
          * the plaintext flag is set in a production environment, or
          * no key is configured and the plaintext flag is not set, or
          * the configured secret is malformed.
        """
        environment = os.environ.get(config.ENVIRONMENT_ENV, "").strip()
        plaintext_requested = config.env_flag_is_true(
            os.environ.get(config.PAYLOAD_CODEC_PLAINTEXT_ENV)
        )
        secret_value = os.environ.get(config.PAYLOAD_CODEC_KEY_ENV, "")

        if plaintext_requested and config.is_production_environment(environment):
            raise PayloadCodecError(
                f"{config.PAYLOAD_CODEC_PLAINTEXT_ENV} is set but "
                f"{config.ENVIRONMENT_ENV}='{environment}' is a production "
                "environment. Unencrypted Temporal payloads are never permitted "
                "in production. Remove the flag and provision "
                f"{config.PAYLOAD_CODEC_KEY_ENV}."
            )

        if secret_value.strip():
            keyring = Keyring.from_secret_value(secret_value)
            logger.info(
                "payload_codec.enabled algorithm=%s primary_key_id=%s "
                "decrypt_key_count=%d; key material not logged",
                ALGORITHM.decode(),
                keyring.primary_key_id,
                len(keyring.keys),
            )
            return cls(keyring)

        if plaintext_requested:
            logger.warning(
                "payload_codec.plaintext_passthrough_enabled environment=%s — "
                "Temporal payloads are NOT encrypted. This is permitted only for "
                "local development and tests; PHI must never be sent through a "
                "worker in this mode.",
                environment or "unset",
            )
            return cls(None, plaintext_passthrough=True)

        raise PayloadCodecError(
            f"{config.PAYLOAD_CODEC_KEY_ENV} is not configured, so Temporal "
            "payloads cannot be encrypted. Provision the payload codec key via "
            "AWS Secrets Manager and inject it in the ECS task definition. For "
            f"local development only, set {config.PAYLOAD_CODEC_PLAINTEXT_ENV}=true."
        )

    # -- introspection ----------------------------------------------------- #
    @property
    def is_encrypting(self) -> bool:
        """True when this codec encrypts on encode (i.e. not in passthrough)."""
        return self._keyring is not None

    @property
    def primary_key_id(self) -> str | None:
        """The key id used to encrypt, or ``None`` in passthrough mode."""
        return self._keyring.primary_key_id if self._keyring else None

    # -- PayloadCodec ------------------------------------------------------ #
    async def encode(self, payloads):  # type: ignore[no-untyped-def]
        """Encrypt each payload, or pass through in explicit local mode.

        Never emits plaintext when a keyring is present, and never falls back to
        plaintext when encryption fails.
        """
        if self._keyring is None:
            # Reachable only via the explicit local-development flag; the
            # constructor refuses a keyless codec otherwise.
            return list(payloads)

        keyring = self._keyring
        key = keyring.primary_key
        key_id = keyring.primary_key_id
        aesgcm = AESGCM(key)

        encoded: list[Payload] = []
        for payload in payloads:
            nonce = os.urandom(NONCE_LENGTH_BYTES)
            ciphertext = aesgcm.encrypt(nonce, payload.SerializeToString(), None)
            encoded.append(
                Payload(
                    metadata={
                        "encoding": ENCRYPTED_ENCODING,
                        METADATA_KEY_ID: key_id.encode("utf-8"),
                        METADATA_ALGORITHM: ALGORITHM,
                    },
                    data=nonce + ciphertext,
                )
            )
        return encoded

    async def decode(self, payloads):  # type: ignore[no-untyped-def]
        """Decrypt payloads this codec produced; pass through everything else.

        A payload marked ``binary/encrypted`` that cannot be decrypted raises.
        A payload with no encrypted marker is returned untouched — it predates
        this codec and was never encrypted (see the module docstring).
        """
        decoded: list[Payload] = []
        for payload in payloads:
            if payload.metadata.get("encoding") != ENCRYPTED_ENCODING:
                decoded.append(payload)
                continue

            algorithm = payload.metadata.get(METADATA_ALGORITHM, b"")
            if algorithm != ALGORITHM:
                raise PayloadCodecError(
                    "encrypted payload declares unsupported algorithm "
                    f"'{algorithm.decode('utf-8', 'replace')}'; this worker "
                    f"implements {ALGORITHM.decode()} only."
                )

            if self._keyring is None:
                raise PayloadCodecError(
                    "received an encrypted Temporal payload but this worker has "
                    "no payload codec key. Provision "
                    f"{config.PAYLOAD_CODEC_KEY_ENV} — plaintext passthrough "
                    "cannot read encrypted history."
                )

            key_id = payload.metadata.get(METADATA_KEY_ID, b"").decode(
                "utf-8", "replace"
            )
            key = self._keyring.key_for(key_id)

            raw = payload.data
            if len(raw) <= NONCE_LENGTH_BYTES:
                raise PayloadCodecError(
                    f"encrypted payload (key id '{key_id}') is truncated: "
                    f"{len(raw)} bytes cannot hold a nonce plus ciphertext."
                )

            nonce, ciphertext = raw[:NONCE_LENGTH_BYTES], raw[NONCE_LENGTH_BYTES:]
            try:
                plaintext = AESGCM(key).decrypt(nonce, ciphertext, None)
            except InvalidTag as exc:
                raise PayloadCodecError(
                    f"encrypted payload (key id '{key_id}') failed its "
                    "authentication tag. The ciphertext was modified or the key "
                    "under that id is not the key that encrypted it."
                ) from exc

            restored = Payload()
            restored.ParseFromString(plaintext)
            decoded.append(restored)
        return decoded


# --------------------------------------------------------------------------
# Data converter wiring
# --------------------------------------------------------------------------


def build_data_converter(
    codec: PayloadCodec | None = None,
) -> DataConverter:
    """Return the Adaptix ``DataConverter``: SDK defaults plus this codec.

    Only the codec slot is replaced. The default payload converter and failure
    converter are kept so JSON/proto/binary values, and Temporal's own failure
    shapes, continue to serialise exactly as they always have — the bytes are
    simply encrypted afterwards.

    Passing ``codec`` explicitly is for tests; production callers omit it and
    get :meth:`EncryptionPayloadCodec.from_environment`, which fails closed.
    """
    return dataclasses.replace(
        DataConverter.default,
        payload_codec=codec
        if codec is not None
        else EncryptionPayloadCodec.from_environment(),
    )
