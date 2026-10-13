package prototype;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.generic.GenericRequestor;

/**
 * Component with a single inbox queue and one worker thread.
 *
 * <p>Reader threads (one per accepted connection) only read frames and enqueue them. The worker is
 * the sole thread invoking the responder, so handler state is serialized by construction.
 *
 * <p>Constraint: call graphs must be free of re-entrant cycles, i.e. a handler must not issue a
 * request that transitively waits on its own worker. Downward chains (A -&gt; B -&gt; C with
 * replies unwinding) are fine.
 */
public abstract class Component {

  protected final String name;
  private final String socketPath;
  private final ServerSocketChannel serverChannel;
  private final Protocol brokerProtocol;
  protected final Protocol protocol;
  private final UnixDomainTransceiver brokerTransceiver;
  private final GenericRequestor requestor;
  private final Responder responder;
  private final AtomicInteger correlationIds = new AtomicInteger(1);
  /** Serializes transceive on the single shared broker connection. */
  private final ReentrantLock brokerLock = new ReentrantLock();
  private final BlockingQueue<QueuedRequest> inbox = new LinkedBlockingQueue<>();
  private volatile Thread worker;

  protected Component(
      String name,
      Path brokerProtocolPath,
      Path protocolPath,
      String brokerSocket,
      String componentSocket)
      throws Exception {
    this.name = name;
    this.socketPath = componentSocket;
    this.serverChannel = startUnixServer(componentSocket);
    this.brokerProtocol = Protocol.parse(brokerProtocolPath.toFile());
    this.protocol = Protocol.parse(protocolPath.toFile());
    this.brokerTransceiver =
        new UnixDomainTransceiver(UnixDomainSocketAddress.of(brokerSocket));
    this.requestor = new GenericRequestor(brokerProtocol, brokerTransceiver);
    this.responder = createResponder(protocol);
    Thread t = Thread.ofVirtual().name(name + "-worker").unstarted(this::serve);
    this.worker = t;
    t.start();
    register();
  }

  /** Define the custom responder used to handle RPC calls for this component. */
  protected abstract Responder createResponder(Protocol protocol);

  public String getName() {
    return name;
  }

  public int nextCorrelationId() {
    return correlationIds.getAndIncrement();
  }

  /** Register this component with the broker. */
  public Object register() throws Exception {
    Protocol.Message message = brokerProtocol.getMessages().get("register");
    GenericRecord params = new GenericData.Record(message.getRequest());
    GenericRecord inner =
        new GenericData.Record(message.getRequest().getField("request").schema());
    inner.put("name", name);
    inner.put("socket", socketPath);
    params.put("request", inner);
    brokerLock.lock();
    try {
      return requestor.request("register", params);
    } finally {
      brokerLock.unlock();
    }
  }

  /** Wrap the request in a message sent through the broker. The payload is opaque to the broker. */
  public Message sendMessage(Message message) throws Exception {
    Protocol.Message rpc = brokerProtocol.getMessages().get("sendMessage");
    GenericRecord params = new GenericData.Record(rpc.getRequest());
    params.put(
        "message",
        MessageCodec.messageToRecord(message, rpc.getRequest().getField("message").schema()));
    Object replyDatum;
    brokerLock.lock();
    try {
      replyDatum = requestor.request("sendMessage", params);
    } finally {
      brokerLock.unlock();
    }
    return MessageCodec.messageFromRecord((GenericRecord) replyDatum);
  }

  private org.apache.avro.Schema requestSchema(String rpcName) {
    Protocol.Message message = protocol.getMessages().get(rpcName);
    if (message == null) {
      throw new IllegalArgumentException(
          "Unknown local RPC '" + rpcName + "'; known: " + protocol.getMessages().keySet().stream().sorted().toList());
    }
    return message.getRequest();
  }

  private org.apache.avro.Schema responseSchema(String rpcName) {
    Protocol.Message message = protocol.getMessages().get(rpcName);
    if (message == null) {
      throw new IllegalArgumentException(
          "Unknown local RPC '" + rpcName + "'; known: " + protocol.getMessages().keySet().stream().sorted().toList());
    }
    return message.getResponse();
  }

  /**
   * Encapsulate an Avro RPC call into an opaque Message. Datum is the full request datum, e.g. a
   * record with field "request" for lock.avpr. The method name is nested inside the payload so the
   * broker stays opaque.
   */
  public Message createMessage(Object datum, String requestName, String destination)
      throws IOException {
    byte[] payload = MessageCodec.encodePayload(requestName, datum, requestSchema(requestName));
    return new Message(name, destination, nextCorrelationId(), payload);
  }

  private Object decodeReply(Message sent, Message reply, String rpcName) throws IOException {
    if (reply.correlationId() != sent.correlationId()) {
      throw new IllegalStateException(
          "Correlation mismatch: sent " + sent.correlationId() + ", got " + reply.correlationId());
    }
    MessageCodec.Envelope envelope = MessageCodec.decodePayloadEnvelope(reply.payload());
    if (!envelope.method().equals(rpcName)) {
      throw new IllegalStateException(
          "RPC method mismatch: sent '" + rpcName + "', got '" + envelope.method() + "'");
    }
    return MessageCodec.decodeAvroDatum(responseSchema(rpcName), envelope.body());
  }

  /** Serve a request addressed to this component without touching the broker. */
  private Message dispatchSelfCall(Message message) throws Exception {
    return MessageCodec.decodeMessage(dispatch(MessageCodec.encodeMessage(message)));
  }

  /** Synchronous RPC through the broker; returns the decoded response datum. */
  public Object callRemote(String destination, String rpcName, GenericRecord params)
      throws Exception {
    Message message = createMessage(params, rpcName, destination);
    System.out.printf("[%s] -> %s rpc=%s id=%d%n", name, destination, rpcName, message.correlationId());
    Message reply;
    if (destination.equals(name)) {
      // Bypass the broker: enqueue from other threads, dispatch inline
      // from the worker itself (enqueue-and-wait there would self-deadlock).
      if (Thread.currentThread() == worker) {
        reply = dispatchSelfCall(message);
      } else {
        CompletableFuture<Message> future = new CompletableFuture<>();
        try {
          inbox.put(new QueuedRequest(MessageCodec.encodeMessage(message), new ReplyChannel(future)));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        try {
          reply = future.get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          throw new RuntimeException(e.getCause());
        }
      }
    } else {
      reply = sendMessage(message);
    }
    return decodeReply(message, reply, rpcName);
  }

  /** Close the connection to the broker. */
  public void close() throws IOException {
    brokerTransceiver.close();
  }

  /** Stop the worker thread; drains items first. */
  public void shutdown() throws InterruptedException {
    inbox.put(QueuedRequest.POISON);
    worker.join(10_000);
  }

  /** Start the unix socket that listens for requests. */
  static ServerSocketChannel startUnixServer(String path) throws IOException {
    Files.deleteIfExists(Path.of(path));
    ServerSocketChannel server = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
    server.bind(UnixDomainSocketAddress.of(path));
    return server;
  }

  /** Listener entry point. */
  public void run() throws IOException {
    while (true) {
      SocketChannel client = serverChannel.accept();
      Thread.ofVirtual().name(name + "-conn").start(() -> handleConnection(client));
    }
  }

  /** Connection handler: reads framed messages and enqueues them for the worker. */
  private void handleConnection(SocketChannel sock) {
    String remoteName;
    try {
      remoteName = "unix:" + sock.getRemoteAddress();
    } catch (IOException e) {
      remoteName = "unix:unknown";
    }
    System.out.printf("[%s] connection from %s%n", name, remoteName);
    ReplyChannel channel = new ReplyChannel(sock);
    try {
      while (true) {
        byte[] raw;
        try {
          raw = FramedIo.readFramed(sock);
        } catch (ClosedChannelException e) {
          System.out.printf("[%s] connection closed: %s%n", name, e);
          break;
        }
        inbox.put(new QueuedRequest(raw, channel));
      }
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      System.out.printf("[%s] connection error: %s%n", name, e);
    } finally {
      channel.lock.lock();
      try {
        channel.closed = true;
      } finally {
        channel.lock.unlock();
      }
      try {
        sock.close();
      } catch (IOException ignored) {
      }
      System.out.printf("[%s] disconnected: %s%n", name, remoteName);
    }
  }

  /** Worker loop: single thread dispatching requests. */
  private void serve() {
    while (true) {
      QueuedRequest item;
      try {
        item = inbox.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      if (item == QueuedRequest.POISON) {
        return;
      }
      byte[] replyRaw;
      try {
        replyRaw = dispatch(item.raw());
      } catch (Exception e) {
        System.out.printf("[%s] request failed: %s%n", name, e);
        fail(item.channel());
        continue;
      }
      try {
        answer(item.channel(), replyRaw);
      } catch (Exception e) {
        System.out.printf("[%s] reply failed: %s%n", name, e);
      }
    }
  }

  /** Return the response to the caller through the channel. */
  private void answer(ReplyChannel channel, byte[] replyRaw) throws IOException {
    if (channel.future != null) {
      if (!channel.future.isDone()) {
        channel.future.complete(MessageCodec.decodeMessage(replyRaw));
      }
      return;
    }
    channel.lock.lock();
    try {
      if (channel.closed) {
        return;
      }
      try {
        FramedIo.writeFramed(channel.channel, replyRaw);
      } catch (IOException e) {
        System.out.printf("[%s] reply dropped, client gone: %s%n", name, e);
        channel.closed = true;
      }
    } finally {
      channel.lock.unlock();
    }
  }

  /**
   * Dispatch failed. Closes the socket so the blocked reader thread and the waiting client both
   * observe EOF instead of hanging.
   */
  private void fail(ReplyChannel channel) {
    if (channel.future != null) {
      if (!channel.future.isDone()) {
        channel.future.completeExceptionally(new RuntimeException("[" + name + "] request failed"));
      }
      return;
    }
    channel.lock.lock();
    try {
      channel.closed = true;
      try {
        channel.channel.close();
      } catch (IOException ignored) {
      }
    } finally {
      channel.lock.unlock();
    }
  }

  /** Decode Message, invoke local responder, return encoded reply. */
  byte[] dispatch(byte[] raw) throws Exception {
    Message message = MessageCodec.decodeMessage(raw);
    MessageCodec.Envelope envelope = MessageCodec.decodePayloadEnvelope(message.payload());
    String method = envelope.method();
    Protocol.Message localMessage = protocol.getMessages().get(method);
    if (localMessage == null) {
      throw new IllegalArgumentException(
          "Unknown RPC '" + method + "' for " + name + "; known: " + protocol.getMessages().keySet().stream().sorted().toList());
    }
    Object requestDatum = MessageCodec.decodeAvroDatum(localMessage.getRequest(), envelope.body());
    System.out.printf("[%s] <- %s rpc=%s id=%d%n", name, message.source(), method, message.correlationId());
    Object responseDatum = responder.respond(localMessage, requestDatum);
    Message reply =
        new Message(
            name,
            message.source(),
            message.correlationId(),
            MessageCodec.encodeResponsePayload(method, responseDatum, localMessage.getResponse()));
    return MessageCodec.encodeMessage(reply);
  }
}
