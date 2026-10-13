package prototype;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import org.apache.avro.ipc.Transceiver;

/**
 * Avro RPC Transceiver over a Unix domain socket. Framing matches python
 * avro.ipc.FramedReader/FramedWriter so Java and Python interoperate.
 */
public class UnixDomainTransceiver extends Transceiver {

  private final SocketChannel channel;
  private final String remoteName;

  public UnixDomainTransceiver(UnixDomainSocketAddress address) throws IOException {
    this(SocketChannel.open(address), "unix:" + address.getPath());
  }

  public UnixDomainTransceiver(SocketChannel channel, String remoteName) {
    this.channel = channel;
    this.remoteName = remoteName;
  }

  public SocketChannel channel() {
    return channel;
  }

  @Override
  public String getRemoteName() {
    return remoteName;
  }

  @Override
  public synchronized List<ByteBuffer> readBuffers() throws IOException {
    return List.of(ByteBuffer.wrap(FramedIo.readFramed(channel)));
  }

  @Override
  public synchronized void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    if (buffers == null) {
      return;
    }
    ByteArrayOutputStream joined = new ByteArrayOutputStream();
    for (ByteBuffer buf : buffers) {
      ByteBuffer dup = buf.duplicate();
      byte[] chunk = new byte[dup.remaining()];
      dup.get(chunk);
      joined.write(chunk);
    }
    FramedIo.writeFramed(channel, joined.toByteArray());
  }

  /**
   * Always stateless, mirroring the Python implementation (which has no connection concept and
   * sends/reads a handshake on every request). This forces {@link
   * org.apache.avro.ipc.Requestor} to include handshake bytes in every request, which is what a
   * stateless responder expects — and it is what Python clients send. Tracking connection state
   * here would break interop: the server would skip reading handshake bytes the client still
   * sends (or vice versa).
   */
  @Override
  public boolean isConnected() {
    return false;
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }
}
