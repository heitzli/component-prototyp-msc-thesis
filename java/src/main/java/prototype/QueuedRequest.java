package prototype;

/** One raw framed request plus the channel to answer on. */
public record QueuedRequest(byte[] raw, ReplyChannel channel) {
  static final QueuedRequest POISON = new QueuedRequest(null, null);
}
