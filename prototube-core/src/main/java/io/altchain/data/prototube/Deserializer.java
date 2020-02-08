package io.altchain.data.prototube;

import java.nio.ByteBuffer;

public interface Deserializer<T> {
  T deserialize(ByteBuffer buf);
}
