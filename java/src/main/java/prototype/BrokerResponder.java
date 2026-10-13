package prototype;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.generic.GenericResponder;

/** Broker RPC handler. Mirrors python/broker.py BrokerResponder. */
public class BrokerResponder extends GenericResponder {

  private final Map<String, String> components = new ConcurrentHashMap<>();

  public BrokerResponder(Protocol local) {
    super(local);
  }

  @Override
  public Object respond(Protocol.Message message, Object request) throws Exception {
    return switch (message.getName()) {
      case "register" -> doRegister(message, (GenericRecord) request);
      case "sendMessage" -> doSendMessage(message, (GenericRecord) request);
      default -> throw new AvroRuntimeException("Unknown RPC method: " + message.getName());
    };
  }

  private GenericRecord doRegister(Protocol.Message message, GenericRecord request) {
    GenericRecord inner = (GenericRecord) request.get("request");
    String name = inner.get("name").toString();
    String socketPath = inner.get("socket").toString();
    components.put(name, socketPath);
    System.out.printf("[broker] registering '%s' at '%s'%n", name, socketPath);
    GenericRecord response = new GenericData.Record(message.getResponse());
    response.put("result", 0);
    return response;
  }

  private GenericRecord doSendMessage(Protocol.Message message, GenericRecord request)
      throws Exception {
    Message routed = MessageCodec.messageFromRecord((GenericRecord) request.get("message"));
    System.out.printf(
        "[broker] route %s -> %s id=%d%n", routed.source(), routed.destination(),
        routed.correlationId());
    Message reply = forward(routed);
    return MessageCodec.messageToRecord(reply, message.getResponse());
  }

  /** Route an opaque Message to its destination and return the reply Message. */
  Message forward(Message message) throws Exception {
    String socketPath = components.get(message.destination());
    if (socketPath == null) {
      throw new AvroRuntimeException(
          "Unknown destination '" + message.destination() + "'; registered: " + components.keySet().stream().sorted().toList());
    }
    try (SocketChannel sock = SocketChannel.open(UnixDomainSocketAddress.of(socketPath))) {
      FramedIo.writeFramed(sock, MessageCodec.encodeMessage(message));
      Message reply = MessageCodec.decodeMessage(FramedIo.readFramed(sock));
      if (reply.correlationId() != message.correlationId()) {
        throw new AvroRuntimeException(
            "Correlation mismatch: sent " + message.correlationId() + ", got " + reply.correlationId());
      }
      System.out.printf(
          "[broker] reply %s -> %s id=%d%n", reply.source(), reply.destination(),
          reply.correlationId());
      return reply;
    }
  }

  /** Start a Unix domain server socket, removing a stale path first. */
  static ServerSocketChannel startUnixServer(String path) throws IOException {
    Files.deleteIfExists(Path.of(path));
    ServerSocketChannel server = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
    server.bind(UnixDomainSocketAddress.of(path));
    return server;
  }

  /** Serve one accepted connection: Avro RPC request/response loop. */
  void handleConnection(SocketChannel channel, String remoteName) {
    System.out.printf("[broker] connection from %s%n", remoteName);
    UnixDomainTransceiver transceiver = new UnixDomainTransceiver(channel, remoteName);
    try {
      while (true) {
        List<ByteBuffer> request;
        try {
          request = transceiver.readBuffers();
        } catch (ClosedChannelException e) {
          System.out.printf("[broker] connection closed: %s%n", e);
          break;
        }
        List<ByteBuffer> response;
        try {
          // Stateless (null connection), like the Python responder: every request carries its
          // own handshake bytes. Passing the transceiver would make the responder skip reading
          // them once "connected", while Python clients keep sending them.
          response = respond(request);
        } catch (Exception e) {
          System.out.printf("[broker] request failed: %s%n", e);
          break;
        }
        if (response != null) {
          try {
            transceiver.writeBuffers(response);
          } catch (ClosedChannelException e) {
            System.out.printf("[broker] connection closed on write: %s%n", e);
            break;
          }
        }
      }
    } catch (IOException e) {
      System.out.printf("[broker] connection error: %s%n", e);
    } finally {
      try {
        transceiver.close();
      } catch (IOException ignored) {
      }
      System.out.printf("[broker] disconnected: %s%n", remoteName);
    }
  }
}
