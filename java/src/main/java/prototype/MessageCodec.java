package prototype;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/** Avro-binary codec for Message + RpcEnvelope. Mirrors python/message_codec.py (CODEC="avro"). */
public final class MessageCodec {

  private MessageCodec() {}

  public static final Schema MESSAGE_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"Message\",\"namespace\":\"common\",\"fields\":["
                  + "{\"name\":\"source\",\"type\":\"string\"},"
                  + "{\"name\":\"destination\",\"type\":\"string\"},"
                  + "{\"name\":\"correlation_id\",\"type\":\"int\"},"
                  + "{\"name\":\"payload\",\"type\":\"bytes\"}]}");

  public static final Schema ENVELOPE_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"RpcEnvelope\",\"fields\":["
                  + "{\"name\":\"method\",\"type\":\"string\"},"
                  + "{\"name\":\"body\",\"type\":\"bytes\"}]}");

  /** Unwrapped payload: method name + raw body bytes (still encoded with the RPC schemas). */
  public record Envelope(String method, byte[] body) {}

  public static byte[] encodeAvroDatum(Schema schema, Object datum) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<>(schema).write(datum, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  public static Object decodeAvroDatum(Schema schema, byte[] raw) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(raw, null);
    return new GenericDatumReader<>(schema, schema).read(null, decoder);
  }

  public static byte[] encodeMessage(Message message) throws IOException {
    GenericRecord record = new GenericData.Record(MESSAGE_SCHEMA);
    record.put("source", message.source());
    record.put("destination", message.destination());
    record.put("correlation_id", message.correlationId());
    record.put("payload", ByteBuffer.wrap(message.payload()));
    return encodeAvroDatum(MESSAGE_SCHEMA, record);
  }

  public static Message decodeMessage(byte[] raw) throws IOException {
    GenericRecord record = (GenericRecord) decodeAvroDatum(MESSAGE_SCHEMA, raw);
    return new Message(
        record.get("source").toString(),
        record.get("destination").toString(),
        (Integer) record.get("correlation_id"),
        toBytes(record.get("payload")));
  }

  /** Wrap an RPC call (method + params datum) into opaque payload bytes. */
  public static byte[] encodePayload(String method, Object paramsDatum, Schema requestSchema)
      throws IOException {
    byte[] body = encodeAvroDatum(requestSchema, paramsDatum);
    GenericRecord envelope = new GenericData.Record(ENVELOPE_SCHEMA);
    envelope.put("method", method);
    envelope.put("body", ByteBuffer.wrap(body));
    return encodeAvroDatum(ENVELOPE_SCHEMA, envelope);
  }

  /** Wrap an RPC response datum into opaque payload bytes. */
  public static byte[] encodeResponsePayload(String method, Object responseDatum, Schema responseSchema)
      throws IOException {
    byte[] body = encodeAvroDatum(responseSchema, responseDatum);
    GenericRecord envelope = new GenericData.Record(ENVELOPE_SCHEMA);
    envelope.put("method", method);
    envelope.put("body", ByteBuffer.wrap(body));
    return encodeAvroDatum(ENVELOPE_SCHEMA, envelope);
  }

  public static Envelope decodePayloadEnvelope(byte[] payload) throws IOException {
    GenericRecord envelope = (GenericRecord) decodeAvroDatum(ENVELOPE_SCHEMA, payload);
    return new Envelope(envelope.get("method").toString(), toBytes(envelope.get("body")));
  }

  /** Convert a Message to a GenericRecord for Avro IPC sendMessage (payload stays opaque bytes). */
  public static GenericRecord messageToRecord(Message message, Schema messageSchema) {
    GenericRecord record = new GenericData.Record(messageSchema);
    record.put("source", message.source());
    record.put("destination", message.destination());
    record.put("correlation_id", message.correlationId());
    record.put("payload", ByteBuffer.wrap(message.payload()));
    return record;
  }

  public static Message messageFromRecord(GenericRecord record) {
    return new Message(
        record.get("source").toString(),
        record.get("destination").toString(),
        (Integer) record.get("correlation_id"),
        toBytes(record.get("payload")));
  }

  static byte[] toBytes(Object avroBytes) {
    if (avroBytes instanceof ByteBuffer buf) {
      ByteBuffer dup = buf.duplicate();
      byte[] bytes = new byte[dup.remaining()];
      dup.get(bytes);
      return bytes;
    }
    if (avroBytes instanceof byte[] bytes) {
      return Arrays.copyOf(bytes, bytes.length);
    }
    throw new IllegalArgumentException("Expected bytes, got: " + avroBytes.getClass());
  }
}
