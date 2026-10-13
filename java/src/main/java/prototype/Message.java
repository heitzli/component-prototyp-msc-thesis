package prototype;

/** Opaque routed message. Mirrors python/message_codec.py Message. */
public record Message(String source, String destination, int correlationId, byte[] payload) {}
