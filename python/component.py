import itertools
import os
import queue
import socket
import threading
from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from socket import socket as Socket
from typing import Any

import avro.ipc
import avro.protocol
from avro.protocol import Protocol

from framedsocket import FramedSocket
from message_codec import (
    CODEC,
    Message,
    decode_avro_datum,
    decode_message,
    decode_payload_envelope,
    encode_message,
    encode_payload,
    encode_response_payload,
    message_from_dict,
    message_to_dict,
)

# TODO: how to handle different protocol sources?
# TODO: connection are closed after succ. rpc call, check how to create long lived connections maybe
# TODO look at work stealing queues in python -> python queue is mpmc


@dataclass
class _ReplyChannel:
    """Write-back path for one accepted connection. Only the worker writes."""

    writer: Any
    lock: threading.Lock = field(default_factory=threading.Lock)
    closed: bool = False
    future: Future[Message] | None = None
    framed_socket: Any = None


@dataclass
class _QueuedRequest:
    raw: bytes
    channel: _ReplyChannel


class Component(ABC):
    """Component with a single inbox queue and one worker thread.

    Reader threads (one per accepted connection) only read frames and enqueue
    them. The worker is the sole thread invoking the responder, so handler
    state is serialized by construction.

    Constraint: call graphs must be free of re-entrant cycles, i.e. a handler
    must not issue a request that transitively waits on its own worker.
    Downward chains (A -> B -> C with replies unwinding) are fine.
    """

    def __init__(
        self, name: str, broker_protocol_path: Path, protocol_path: Path, broker_socket: str, component_socket: str
    ) -> None:
        self.name = name
        self.socket_path: str = component_socket
        self.sock = self.start_unix_server(path=component_socket)
        self.broker_protocol = self._load_protocol(protocol_path=broker_protocol_path)
        self.protocol = self._load_protocol(protocol_path=protocol_path)

        self.broker_connection = self._connect_to_broker(broker_socket=broker_socket)

        self.requestor = avro.ipc.Requestor(self.broker_protocol, self.broker_connection)
        # Worker-thread-confined: only the worker thread calls responder.invoke, so responders need no internal locking for handler state.
        self.responder = self._create_responder(self.protocol)
        self._correlation_ids = itertools.count(1)
        # Serializes transceive (write-frame + blocking-read-frame) on the single shared broker_connection; without it concurrent call_remote() calls can read each other's responses.
        self._broker_lock = threading.Lock()
        self._inbox: queue.Queue[_QueuedRequest | None] = queue.Queue()
        self._worker_ident: int | None = None
        self._worker = threading.Thread(target=self._serve, daemon=True, name=f"{name}-worker")
        self._worker.start()
        self.register()

    @abstractmethod
    def _create_responder(self, protocol: Protocol) -> avro.ipc.Responder:
        """Define custom responder class that is used to handle rpc calls by the component."""
        ...

    def _load_protocol(self, protocol_path: Path) -> Protocol:
        """Helper function to load the protocol implemented by the component."""
        with open(protocol_path, "r", encoding="utf-8") as f:
            return avro.protocol.parse(f.read())

    def _connect_to_broker(self, broker_socket: str) -> FramedSocket:
        """Create longlived connection to broker."""
        sock = Socket(family=socket.AF_UNIX, type=socket.SOCK_STREAM)
        sock.connect(broker_socket)
        return FramedSocket(sock=sock, remote_name=f"unix:{broker_socket}")

    def next_correlation_id(self) -> int:
        """Create correlation ids to match messages, currently not strictly necessary but might be helpful in the future."""
        return next(self._correlation_ids)

    def register(self) -> Any:
        """Register the component to the broker. Maybe add more metadata in the future depending on the role of the broker."""
        return self.requestor.request(
            "register",
            {
                "request": {
                    "name": self.name,
                    "socket": self.socket_path,
                }
            },
        )

    def send_message(self, message: Message) -> Message:
        """Wrap the request in a message that is sent through the broker to the destination. The payload is opaque to the broker."""
        # Serializes all requestor.request calls on the shared broker_connection.
        with self._broker_lock:
            reply_datum = self.requestor.request(
                message_name="sendMessage",
                request_datum={"message": message_to_dict(message)},
            )
        return message_from_dict(reply_datum)

    def _request_schema(self, rpc_name: str) -> Any:
        """Helper function to retrieve the request schema"""
        if messages := self.protocol.messages:
            avro_message = messages.get(rpc_name)
        else:
            raise ValueError("Error retrieving messages.")
        if avro_message is None:
            raise ValueError(f"Unknown local RPC {rpc_name!r}; known: {sorted(self.protocol.messages)}")
        return avro_message.request

    def _response_schema(self, rpc_name: str) -> Any:
        """Helper function to retrieve the response schema"""
        if messages := self.protocol.messages:
            avro_message = messages.get(rpc_name)
        else:
            raise ValueError("Error retrieving messages.")
        if avro_message is None:
            raise ValueError(f"Unknown local RPC {rpc_name!r}; known: {sorted(self.protocol.messages)}")
        return avro_message.response

    def create_message(self, datum: dict, request_name: str, destination: str) -> Message:
        """Encapsulate an Avro RPC call into an opaque Message.

        datum is the full request datum, e.g. {"request": {...}} for lock.avpr.
        The method name is nested inside payload so the broker stays opaque.
        """
        payload = encode_payload(request_name, datum, self._request_schema(request_name))
        return Message(
            source=self.name,
            destination=destination,
            correlation_id=self.next_correlation_id(),
            payload=payload,
        )

    def _decode_reply(self, message: Message, reply: Message, rpc_name: str) -> Any:
        """Decode the rpc reply, correlation id currently not necessary but might be needed in the future."""
        if reply.correlation_id != message.correlation_id:
            raise RuntimeError(f"Correlation mismatch: sent {message.correlation_id}, got {reply.correlation_id}")
        method, body = decode_payload_envelope(reply.payload)
        if method != rpc_name:
            raise RuntimeError(f"RPC method mismatch: sent {rpc_name!r}, got {method!r}")
        if CODEC == "avro":
            return decode_avro_datum(self._response_schema(rpc_name), body)
        return body

    def _dispatch_self_call(self, message: Message) -> Message:
        """Serve a request addressed to this component without touching the broker."""
        reply_raw = self._dispatch(encode_message(message))
        return decode_message(reply_raw)

    def call_remote(self, destination: str, rpc_name: str, params: dict) -> Any:
        """Synchronous RPC through the broker returns the decoded response datum."""
        message = self.create_message(datum=params, request_name=rpc_name, destination=destination)
        print(f"[{self.name}] -> {destination} rpc={rpc_name} id={message.correlation_id}", flush=True)
        if destination == self.name:
            # Bypass the broker: enqueue from other threads, dispatch inline
            # from the worker itself (enqueue-and-wait there would self-deadlock).
            if threading.get_ident() == self._worker_ident:
                reply = self._dispatch_self_call(message)
            else:
                future: Future[Message] = Future()
                self._inbox.put(_QueuedRequest(encode_message(message), _ReplyChannel(writer=None, future=future)))
                reply = future.result()
        else:
            reply = self.send_message(message)
        return self._decode_reply(message, reply, rpc_name)

    def close(self) -> None:
        """Close the connection to the broker."""
        self.broker_connection.close()

    def shutdown(self) -> None:
        """Stop the worker thread, drains items first."""
        self._inbox.put(None)
        self._worker.join(timeout=10)

    def start_unix_server(self, path: str) -> Socket:
        """Start the unix socket that listens to requests."""
        if os.path.exists(path):
            os.unlink(path)
        server = socket.socket(
            family=socket.AF_UNIX,
            type=socket.SOCK_STREAM,
        )
        server.bind(path)
        server.listen()
        return server

    def run(self) -> None:
        """Listener entry point."""
        while True:
            client_sock, address = self.sock.accept()
            thread = threading.Thread(target=self._handle_connection, args=(client_sock, address), daemon=True)
            thread.start()

    def _handle_connection(self, sock: socket.socket, address: str) -> None:
        """Connection handler, reads framed socket"""
        print(f"[{self.name}] connection from {address}", flush=True)
        framed_socket = FramedSocket(sock, remote_name=f"unix:{address}")
        reader = avro.ipc.FramedReader(framed_socket)
        channel = _ReplyChannel(writer=avro.ipc.FramedWriter(framed_socket), framed_socket=framed_socket)

        try:
            while True:
                try:
                    raw = reader.read_framed_message()
                except Exception as exc:
                    print(f"[{self.name}] connection closed: {exc}", flush=True)
                    break
                if not raw:
                    break
                self._inbox.put(_QueuedRequest(raw, channel))
        finally:
            with channel.lock:
                channel.closed = True
            framed_socket.close()
            print(f"[{self.name}] disconnected: {address}", flush=True)

    def _serve(self) -> None:
        """Worker loop: one worker thread for the moment that dispatches requests."""
        self._worker_ident = threading.get_ident()
        while True:
            item = self._inbox.get()
            try:
                if item is None:
                    return
                try:
                    reply_raw = self._dispatch(item.raw)
                except Exception as exc:
                    print(f"[{self.name}] request failed: {exc}", flush=True)
                    self._fail(item.channel)
                    continue
                self._answer(item.channel, reply_raw)
            finally:
                self._inbox.task_done()

    def _answer(self, channel: _ReplyChannel, reply_raw: bytes) -> None:
        """Returns the response to the callee through the channel."""
        if channel.future is not None:
            if not channel.future.done():
                channel.future.set_result(decode_message(reply_raw))
            return
        assert channel.writer is not None
        with channel.lock:
            if channel.closed:
                return
            try:
                channel.writer.write_framed_message(message=reply_raw)
            except (ConnectionError, BrokenPipeError, OSError) as exc:
                print(f"[{self.name}] reply dropped, client gone: {exc}", flush=True)
                channel.closed = True

    def _fail(self, channel: _ReplyChannel) -> None:
        """Dispatch failed. Closes the socket so the blocked reader thread and the waiting client
        both observe EOF instead of hanging.
        """
        if channel.future is not None:
            if not channel.future.done():
                channel.future.set_exception(RuntimeError(f"[{self.name}] request failed"))
            return
        with channel.lock:
            channel.closed = True
            if channel.framed_socket is not None:
                try:
                    channel.framed_socket.close()
                except OSError:
                    pass

    def _dispatch(self, raw: bytes) -> bytes:
        """Decode Message, invoke local responder, return encoded reply."""
        message = decode_message(raw)
        method, body = decode_payload_envelope(message.payload)
        if messages := self.protocol.messages:
            local_message = messages.get(method)
        else:
            raise ValueError("Error retrieving messages.")
        if local_message is None:
            raise ValueError(f"Unknown RPC {method!r} for {self.name}; known: {sorted(self.protocol.messages)}")
        if CODEC == "avro":
            request_datum = decode_avro_datum(local_message.request, body)
        else:
            request_datum = body
        print(f"[{self.name}] <- {message.source} rpc={method} id={message.correlation_id}", flush=True)
        response_datum = self.responder.invoke(local_message, request_datum)
        reply = Message(
            source=self.name,
            destination=message.source,
            correlation_id=message.correlation_id,
            payload=encode_response_payload(method, response_datum, local_message.response),
        )
        return encode_message(reply)
