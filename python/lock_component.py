from enum import Enum
from pathlib import Path

import avro.ipc
from avro.protocol import Protocol

from component import Component


class LockType(str, Enum):
    Shared = "Shared"
    Exclusive = "Exclusive"


class LockResponse(str, Enum):
    Ok = "Ok"
    TxAbort = "TxAbort"


class LockManagerResponder(avro.ipc.Responder):
    def __init__(self, local_protocol):
        super().__init__(local_protocol)

    def invoke(self, local_message, request):  # type: ignore
        method = local_message.name

        if method == "requestLock":
            return self.request_lock(request)

        raise RuntimeError(f"Unknown RPC method: {method}")

    def request_lock(self, request):
        inner = request["request"]
        tx_id = inner["transactionID"]
        lock_type = LockType(value=inner["locktype"])
        data_item = inner["dataItem"]

        # TODO lock logic
        print(f"Tx {tx_id} requests lock of type {lock_type} on data item {data_item}")
        return {"response": LockResponse.Ok}


class LockManager(Component):
    def __init__(
        self,
        name: str,
        broker_protocol_path: Path,
        protocol_path: Path,
        broker_socket: str,
        component_socket: str,
    ) -> None:
        super().__init__(name, broker_protocol_path, protocol_path, broker_socket, component_socket)

    def _create_responder(self, protocol: Protocol) -> avro.ipc.Responder:
        return LockManagerResponder(local_protocol=self.protocol)


if __name__ == "__main__":
    lm = LockManager(
        name="LockManager",
        broker_protocol_path=Path("../protocol/broker.avpr").absolute(),
        protocol_path=Path("../protocol/lock.avpr").absolute(),
        broker_socket="/tmp/broker.sock",
        component_socket="/tmp/lock_manager.sock",
    )

    lm.run()
