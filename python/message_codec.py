from __future__ import annotations

import base64
import io
import json
from dataclasses import dataclass
from typing import Any

import avro.io
import avro.schema

# Global toggle, either avro (binary), or json (for debugging)
CODEC = "avro"
# CODEC = "json"

MESSAGE_SCHEMA = avro.schema.parse(
    json.dumps(
        {
            "type": "record",
            "name": "Message",
            "namespace": "common",
            "fields": [
                {"name": "source", "type": "string"},
                {"name": "destination", "type": "string"},
                {"name": "correlation_id", "type": "int"},
                {"name": "payload", "type": "bytes"},
            ],
        }
    )
)

ENVELOPE_SCHEMA = avro.schema.parse(
    json.dumps(
        {
            "type": "record",
            "name": "RpcEnvelope",
            "fields": [
                {"name": "method", "type": "string"},
                {"name": "body", "type": "bytes"},
            ],
        }
    )
)


@dataclass
class Message:
    source: str
    destination: str
    correlation_id: int
    payload: bytes


def encode_avro_datum(schema: avro.schema.Schema, datum: Any) -> bytes:
    """Encode the datum given a schema."""
    buffer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(buffer)
    avro.io.DatumWriter(schema).write(datum, encoder)
    return buffer.getvalue()


def decode_avro_datum(schema: avro.schema.Schema, raw: bytes) -> Any:
    """Decode raw bytes given a schema."""
    decoder = avro.io.BinaryDecoder(io.BytesIO(raw))
    return avro.io.DatumReader(schema, schema).read(decoder)


def encode_message_avro(message: Message) -> bytes:
    """Encode data into binary avro."""
    return encode_avro_datum(
        MESSAGE_SCHEMA,
        {
            "source": message.source,
            "destination": message.destination,
            "correlation_id": message.correlation_id,
            "payload": message.payload,
        },
    )


def decode_message_avro(raw: bytes) -> Message:
    """Decode data from binary avro."""
    datum = decode_avro_datum(MESSAGE_SCHEMA, raw)
    return Message(
        source=datum["source"],
        destination=datum["destination"],
        correlation_id=datum["correlation_id"],
        payload=datum["payload"],
    )


def encode_message_json(message: Message) -> bytes:
    """Encode data into json avro"""
    return json.dumps(
        {
            "source": message.source,
            "destination": message.destination,
            "correlation_id": message.correlation_id,
            "payload": base64.b64encode(message.payload).decode("ascii"),
        }
    ).encode("utf-8")


def decode_message_json(raw: bytes) -> Message:
    """Decode raw data to json avro."""
    datum = json.loads(raw.decode("utf-8"))
    return Message(
        source=datum["source"],
        destination=datum["destination"],
        correlation_id=datum["correlation_id"],
        payload=base64.b64decode(datum["payload"]),
    )


def encode_message(message: Message) -> bytes:
    """Encode a Message for the broker component socket using the global CODEC."""
    if CODEC == "avro":
        return encode_message_avro(message)
    return encode_message_json(message)


def decode_message(raw: bytes) -> Message:
    """Decode a Message from the broker component socket using the global CODEC."""
    if CODEC == "avro":
        return decode_message_avro(raw)
    return decode_message_json(raw)


def encode_payload(method: str, params_datum: dict[str, Any], request_schema: Any) -> bytes:
    """Wrap an RPC call (method + params) into opaque payload bytes."""
    if CODEC == "avro":
        body = encode_avro_datum(request_schema, params_datum)
        return encode_avro_datum(ENVELOPE_SCHEMA, {"method": method, "body": body})
    return json.dumps({"method": method, "body": params_datum}).encode("utf-8")


def decode_payload_envelope(payload: bytes) -> tuple[str, Any]:
    """Unwrap payload into (method, body) where body is bytes (avro) or dict (json)."""
    if CODEC == "avro":
        datum = decode_avro_datum(ENVELOPE_SCHEMA, payload)
        return datum["method"], datum["body"]
    datum = json.loads(payload.decode("utf-8"))
    return datum["method"], datum["body"]


def encode_response_payload(method: str, response_datum: Any, response_schema: Any) -> bytes:
    """Wrap an RPC response into opaque payload bytes."""
    if CODEC == "avro":
        body = encode_avro_datum(response_schema, response_datum)
        return encode_avro_datum(ENVELOPE_SCHEMA, {"method": method, "body": body})
    return json.dumps({"method": method, "body": response_datum}).encode("utf-8")


def message_to_dict(message: Message) -> dict[str, Any]:
    """Convert to a plain dict for Avro IPC ``sendMessage`` payload remains opaque, e.g. bytes"""
    return {
        "source": message.source,
        "destination": message.destination,
        "correlation_id": message.correlation_id,
        "payload": message.payload,
    }


def message_from_dict(datum: dict[str, Any]) -> Message:
    """Helper function to create message from dictionary datum."""
    return Message(
        source=datum["source"],
        destination=datum["destination"],
        correlation_id=datum["correlation_id"],
        payload=datum["payload"],
    )
