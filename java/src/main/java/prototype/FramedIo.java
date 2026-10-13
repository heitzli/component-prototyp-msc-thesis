package prototype;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.List;

/**
 * Length-prefixed framing over a byte stream, matching python avro.ipc
 * FramedReader/FramedWriter (big-endian int32 lengths, zero-length terminator).
 */
public final class FramedIo {

  private FramedIo() {}

  static final int BUFFER_SIZE = 8192;

  private static void readFully(SocketChannel channel, ByteBuffer buf) throws IOException {
    while (buf.hasRemaining()) {
      int n = channel.read(buf);
      if (n < 0) {
        throw new ClosedChannelException();
      }
    }
  }

  private static void writeFully(SocketChannel channel, ByteBuffer buf) throws IOException {
    while (buf.hasRemaining()) {
      channel.write(buf);
    }
  }

  /** Read one framed message (all frames joined). Throws ClosedChannelException on EOF. */
  public static byte[] readFramed(SocketChannel channel) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteBuffer header = ByteBuffer.allocate(4);
    while (true) {
      header.clear();
      try {
        readFully(channel, header);
      } catch (ClosedChannelException e) {
        if (out.size() == 0) {
          throw e;
        }
        throw new EOFException("socket closed mid-frame");
      }
      header.flip();
      int length = header.getInt();
      if (length == 0) {
        return out.toByteArray();
      }
      if (length < 0) {
        throw new IOException("negative frame length: " + length);
      }
      ByteBuffer chunk = ByteBuffer.allocate(length);
      readFully(channel, chunk);
      chunk.flip();
      out.write(chunk.array(), chunk.arrayOffset() + chunk.position(), chunk.remaining());
    }
  }

  /** Write one framed message, chunked at 8k like the Python writer. */
  public static void writeFramed(SocketChannel channel, byte[] message) throws IOException {
    int offset = 0;
    while (offset < message.length) {
      int n = Math.min(BUFFER_SIZE, message.length - offset);
      writeHeader(channel, n);
      writeFully(channel, ByteBuffer.wrap(message, offset, n));
      offset += n;
    }
    writeHeader(channel, 0);
  }

  /** Write pre-framed Avro buffers as a single message (frames concatenated, one terminator). */
  public static void writeBuffersAsMessage(SocketChannel channel, List<ByteBuffer> buffers)
      throws IOException {
    for (ByteBuffer buf : buffers) {
      ByteBuffer dup = buf.duplicate();
      while (dup.hasRemaining()) {
        int n = Math.min(BUFFER_SIZE, dup.remaining());
        byte[] chunk = new byte[n];
        dup.get(chunk);
        writeHeader(channel, n);
        writeFully(channel, ByteBuffer.wrap(chunk));
      }
    }
    writeHeader(channel, 0);
  }

  private static void writeHeader(SocketChannel channel, int length) throws IOException {
    ByteBuffer header = ByteBuffer.allocate(4);
    header.putInt(length);
    header.flip();
    writeFully(channel, header);
  }
}
