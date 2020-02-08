package io.altchain.data.prototube.kafka;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import io.altchain.data.idl.Prototube;
import io.altchain.data.prototube.MessageDeserializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

class MessageSerializer {
  private static final int VERSION = 1;
  private static final int UUID_LENGTH = 16;
  private static final Prototube.PrototubeMessageHeader HEADER_TEMPLATE = Prototube.PrototubeMessageHeader.newBuilder()
          .setVersion(VERSION)
          .setTs(0)
          .setUuid(ByteString.copyFrom(new byte[UUID_LENGTH]))
          .buildPartial();
  private static final int HEADER_SIZE = getDelimitedMessageSize(HEADER_TEMPLATE);

  byte[] serialize(Message message, long time) {
    UUID uuid = UUID.randomUUID();
    ByteBuffer bb = ByteBuffer.wrap(new byte[UUID_LENGTH]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    bb.flip();

    Prototube.PrototubeMessageHeader header = Prototube.PrototubeMessageHeader.newBuilder(HEADER_TEMPLATE)
            .setTs(time)
            .setUuid(ByteString.copyFrom(bb))
            .build();
    ByteArrayOutputStream os = new ByteArrayOutputStream(getRecordSize(message));
    try {
      os.write(MessageDeserializer.HEADER);
      header.writeDelimitedTo(os);
      message.writeDelimitedTo(os);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return os.toByteArray();
  }

  private static int getRecordSize(Message message) {
    return MessageDeserializer.HEADER.length + HEADER_SIZE + getDelimitedMessageSize(message);
  }

  private static int getDelimitedMessageSize(Message msg) {
    int serialized = msg.getSerializedSize();
    return CodedOutputStream.computeUInt32SizeNoTag(serialized) + serialized;
  }
}
