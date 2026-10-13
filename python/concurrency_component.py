import threading
import time
from pathlib import Path

import avro.ipc
from avro.protocol import Protocol

from component import Component


class ConcurrencyResponder(avro.ipc.Responder):
    # TODO: Placeholder
    def __init__(self, local_protocol):
        super().__init__(local_protocol)

    def invoke(self, local_message, request):  # type: ignore
        raise RuntimeError(f"ConcurrencyManager serves no RPC; got {local_message.name!r}")


class ConcurrencyManager(Component):
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
        return ConcurrencyResponder(local_protocol=self.protocol)

    def request_lock(self, tx_id: int = 1, data_item: str = "x", locktype: str = "Exclusive") -> None:
        response = self.call_remote(
            "LockManager",
            "requestLock",
            {"request": {"transactionID": tx_id, "locktype": locktype, "dataItem": data_item}},
        )
        print(f"[ConcurrencyManager] lock response: {response}", flush=True)


if __name__ == "__main__":
    lm = ConcurrencyManager(
        name="ConcurrencyManager",
        broker_protocol_path=Path("../protocol/broker.avpr").absolute(),
        protocol_path=Path("../protocol/lock.avpr").absolute(),
        broker_socket="/tmp/broker.sock",
        component_socket="/tmp/concurrency_manager.sock",
    )

    server = threading.Thread(target=lm.run, daemon=True)
    server.start()
    for attempt in range(10):
        try:
            lm.request_lock()

        except Exception as exc:  # LockManager not registered yet TODO keep for demo, remove in future
            print(f"[ConcurrencyManager] retry {attempt}: {exc}", flush=True)
            time.sleep(0.5)
