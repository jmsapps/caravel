"""Snapshot codecs.

A snapshot is stored as bytes beside the identity of the codec that produced
it, and it is decoded with that stored identity. A dataset declares a codec by
exposing ``snapshot_codec()``; the JSON and text datasets use the ``json/utf-8``
default.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Protocol, runtime_checkable

JSON_UTF8 = "json/utf-8"


@dataclass(frozen=True)
class SnapshotCodec:
    """An identity and the pair of functions that carry a payload through it."""

    identity: str
    encode: Callable[[Any], bytes]
    decode: Callable[[bytes], Any]


@runtime_checkable
class CodecDeclaringDataset(Protocol):
    """A dataset that names the codec its payloads are stored with."""

    def snapshot_codec(self) -> SnapshotCodec: ...


def _encode_json(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")


def _decode_json(blob: bytes) -> Any:
    return json.loads(blob.decode("utf-8"))


JSON_UTF8_CODEC = SnapshotCodec(identity=JSON_UTF8, encode=_encode_json, decode=_decode_json)


class CodecError(Exception):
    """Raised when a payload has no codec, or a stored identity has no codec."""


class SnapshotCodecs:
    """The codecs one run can encode with and decode through.

    Every dataset in the declaration contributes its codec, so decoding a stored
    identity means looking up a codec the current declaration still names.
    """

    def __init__(self) -> None:
        self._by_identity: dict[str, SnapshotCodec] = {JSON_UTF8: JSON_UTF8_CODEC}

    def register(self, codec: SnapshotCodec) -> None:
        self._by_identity[codec.identity] = codec

    def for_dataset(self, dataset: Any, *, step_name: str) -> SnapshotCodec:
        """The codec a dataset's payloads are stored with."""
        declared = getattr(dataset, "snapshot_codec", None)
        if callable(declared):
            codec = declared()
            if not isinstance(codec, SnapshotCodec):
                raise CodecError(
                    f"Dataset for step '{step_name}' declared "
                    f"{type(codec).__name__} as its snapshot codec. Declare a "
                    "SnapshotCodec with an identity and an encode/decode pair."
                )
            self.register(codec)
            return codec

        raise CodecError(
            f"Dataset {type(dataset).__name__} for step '{step_name}' stores values "
            "that need an explicit codec. Declare snapshot_codec() on the dataset "
            "with an identity and an encode/decode pair."
        )

    def by_identity(self, identity: str, *, step_name: str) -> SnapshotCodec:
        """The codec a stored snapshot names."""
        codec = self._by_identity.get(identity)
        if codec is None:
            raise CodecError(
                f"Step '{step_name}' holds a snapshot stored with codec "
                f"{identity!r}, which this pipeline's declarations do not name. "
                "Declare that codec to read the snapshot."
            )
        return codec


__all__ = [
    "JSON_UTF8",
    "JSON_UTF8_CODEC",
    "CodecDeclaringDataset",
    "CodecError",
    "SnapshotCodec",
    "SnapshotCodecs",
]
