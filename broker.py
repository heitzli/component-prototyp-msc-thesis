from __future__ import annotations

import os
import socket
import threading
from pathlib import Path
from socket import socket as Socket
from typing import TYPE_CHECKING, Any, TypeAlias

import avro.ipc
import avro.protocol

from framedsocket import FramedSocket
from message_codec import Message, decode_message, encode_message, message_from_dict, message_to_dict

if TYPE_CHECKING:
    from avro.protocol import Protocol


RequestDict: TypeAlias = dict[str, Any]
ResultDict: TypeAlias = dict[str, Any]


class BrokerResponder(avro.ipc.Responder):
    def __init__(self, protocol: avro.protocol.Protocol, broker: Broker) -> None:
        super().__init__(local_protocol=protocol)
        self._broker: Broker = broker

    def invoke(self, message: avro.protocol.Message, request: RequestDict):  # type: ignore
        method = message.name

        if method == "register":
            return self.register(request)

        if method == "sendMessage":
            return self.send_message(request)

        raise RuntimeError(f"Unknown RPC method: {method}")

    def send_message(self, request: RequestDict) -> ResultDict:
        message = message_from_dict(request["message"])
        print(f"[broker] route {message.source} -> {message.destination} id={message.correlation_id}", flush=True)
        reply = self._broker.forward(message)
        return message_to_dict(reply)

    def register(self, request: RequestDict) -> dict[str, int]:
        inner = request["request"]
        name = inner["name"]
        socket_path = inner["socket"]
        # TODO: check if overriding logic is wanted
        self._broker.components[name] = socket_path

        print(f"[broker] registering {name!r} at {socket_path!r}", flush=True)

        return {"result": 0}


class Broker:
    def __init__(self, protocol_path: Path) -> None:
        # Inject broker into broker responder
        self.responder = BrokerResponder(protocol=self._load_protocol(protocol_path=protocol_path), broker=self)
        self.sock = self.start_unix_server(path="/tmp/broker.sock")
        self.components: dict[str, str] = dict()

    def forward(self, message: Message) -> Message:
        """Route an opaque Message to its destination and return the reply Message."""
        socket_path = self.components.get(message.destination)
        if socket_path is None:
            raise KeyError(f"Unknown destination {message.destination!r}; registered: {sorted(self.components)}")

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(socket_path)
            framed_socket = FramedSocket(sock, remote_name=f"unix:{socket_path}")
            framed_socket.writer.write_framed_message(message=encode_message(message))
            reply = decode_message(framed_socket.reader.read_framed_message())

        if reply.correlation_id != message.correlation_id:
            raise RuntimeError(f"Correlation mismatch: sent {message.correlation_id}, got {reply.correlation_id}")
        print(f"[broker] reply {reply.source} -> {reply.destination} id={reply.correlation_id}", flush=True)
        return reply

    def _load_protocol(self, protocol_path: Path) -> Protocol:
        with open(file=str(protocol_path.absolute()), mode="r", encoding="utf-8") as f:
            return avro.protocol.parse(json_string=f.read())

    def start_unix_server(self, path: str) -> Socket:
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
        while True:
            client_sock, address = self.sock.accept()
            thread = threading.Thread(target=self._handle_connection, args=(client_sock, address), daemon=True)
            thread.start()

    def _handle_connection(self, sock: socket.socket, address: str) -> None:
        print(f"[broker] connection from {address}", flush=True)
        framed_socket = FramedSocket(sock, remote_name=f"unix:{address}")
        reader = avro.ipc.FramedReader(framed_socket)
        writer = avro.ipc.FramedWriter(framed_socket)

        try:
            while True:
                try:
                    request = reader.read_framed_message()
                except Exception as exc:
                    print(f"[broker] connection closed: {exc}", flush=True)
                    break
                if not request:
                    break
                try:
                    response = self.responder.respond(call_request=request)
                    writer.write_framed_message(message=response)
                except Exception as exc:
                    print(f"[broker] request failed: {exc}", flush=True)
                    raise
        finally:
            framed_socket.close()
            print(f"[broker] disconnected: {address}", flush=True)


if __name__ == "__main__":
    b = Broker(protocol_path=Path("protocol/brocker.avpr"))
    b.run()
