package prototype;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import org.apache.avro.Protocol;

/** Broker entry point. Mirrors python/broker.py Broker. */
public class Broker {

  private final BrokerResponder responder;
  private final ServerSocketChannel serverChannel;

  public Broker(Path protocolPath, String socketPath) throws Exception {
    Protocol protocol = Protocol.parse(protocolPath.toFile());
    this.responder = new BrokerResponder(protocol);
    this.serverChannel = BrokerResponder.startUnixServer(socketPath);
  }

  public void run() throws Exception {
    while (true) {
      SocketChannel client = serverChannel.accept();
      String remoteName;
      try {
        remoteName = "unix:" + client.getRemoteAddress();
      } catch (Exception e) {
        remoteName = "unix:unknown";
      }
      String name = remoteName;
      Thread.ofVirtual().name("broker-conn").start(() -> responder.handleConnection(client, name));
    }
  }

  public static void main(String[] args) throws Exception {
    String protocolArg = args.length > 0 ? args[0] : "../protocol/broker.avpr";
    String socketArg = args.length > 1 ? args[1] : "/tmp/broker.sock";
    new Broker(Path.of(protocolArg), socketArg).run();
  }
}
