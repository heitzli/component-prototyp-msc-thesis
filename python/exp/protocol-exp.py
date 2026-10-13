from pathlib import Path

import avro.protocol
from avro.protocol import Protocol

# TODO: maybe can use the functionality to retrieve on the fly schemas etc from broker that has for each component their protocol registered.


def _load_protocol(protocol_path: Path) -> Protocol:
    with open(file=str(protocol_path.absolute()), mode="r", encoding="utf-8") as f:
        return avro.protocol.parse(json_string=f.read())


proto = _load_protocol(Path("protocol/broker.avpr"))
print(proto.fullname)
print(proto.md5)
print(proto.name)
print(proto.namespace)
print(proto.types)
print(proto.messages)
