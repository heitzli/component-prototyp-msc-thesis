package prototype;

import java.nio.file.Path;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.generic.GenericResponder;

/** Lock manager component. Mirrors python/lock_component.py. */
public class LockManager extends Component {

  /** Mirrors the LockType enum in protocol/lock.avpr. */
  public enum LockType {
    Shared,
    Exclusive
  }

  /** Mirrors the LockResponse enum in protocol/lock.avpr. */
  public enum LockResponse {
    Ok,
    TxAbort
  }

  static class LockManagerResponder extends GenericResponder {
    LockManagerResponder(Protocol local) {
      super(local);
    }

    @Override
    public Object respond(Protocol.Message message, Object request) throws Exception {
      if (message.getName().equals("requestLock")) {
        return requestLock(message, (GenericRecord) request);
      }
      throw new AvroRuntimeException("Unknown RPC method: " + message.getName());
    }

    private GenericRecord requestLock(Protocol.Message message, GenericRecord request) {
      GenericRecord inner = (GenericRecord) request.get("request");
      int txId = (Integer) inner.get("transactionID");
      LockType lockType = LockType.valueOf(inner.get("locktype").toString());
      String dataItem = inner.get("dataItem").toString();

      // TODO lock logic
      System.out.printf("Tx %d requests lock of type %s on data item %s%n", txId, lockType, dataItem);

      GenericRecord response = new GenericData.Record(message.getResponse());
      response.put(
          "response",
          new GenericData.EnumSymbol(
              message.getResponse().getField("response").schema(), LockResponse.Ok.name()));
      return response;
    }
  }

  public LockManager(
      String name,
      Path brokerProtocolPath,
      Path protocolPath,
      String brokerSocket,
      String componentSocket)
      throws Exception {
    super(name, brokerProtocolPath, protocolPath, brokerSocket, componentSocket);
  }

  @Override
  protected Responder createResponder(Protocol protocol) {
    return new LockManagerResponder(protocol);
  }

  public static void main(String[] args) throws Exception {
    String brokerProtocol = args.length > 0 ? args[0] : "../protocol/broker.avpr";
    String protocol = args.length > 1 ? args[1] : "../protocol/lock.avpr";
    String brokerSocket = args.length > 2 ? args[2] : "/tmp/broker.sock";
    String componentSocket = args.length > 3 ? args[3] : "/tmp/lock_manager.sock";
    new LockManager(
            "LockManager",
            Path.of(brokerProtocol),
            Path.of(protocol),
            brokerSocket,
            componentSocket)
        .run();
  }
}
