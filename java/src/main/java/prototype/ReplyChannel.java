package prototype;

import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/** Write-back path for one accepted connection. Only the worker writes. */
public final class ReplyChannel {
  final SocketChannel channel;
  final ReentrantLock lock = new ReentrantLock();
  volatile boolean closed;
  final CompletableFuture<Message> future;

  public ReplyChannel(SocketChannel channel) {
    this.channel = channel;
    this.future = null;
  }

  public ReplyChannel(CompletableFuture<Message> future) {
    this.channel = null;
    this.future = future;
  }
}
