package io.altchain.data.prototube;

import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

public class JsonDeserializer extends MessageDeserializer implements Deserializer<String> {
  private static final long serialVersionUID = 0L;

  public JsonDeserializer(String payloadClassName) throws NoSuchMethodException, InvocationTargetException,
          IllegalAccessException, ClassNotFoundException {
    super(payloadClassName);
  }

  public String deserialize(ByteBuffer buf) {
    try {
      boolean r = deserializeMessage(buf);
      if (!r) {
        return null;
      }
      return JsonFormat.printer().print(record);
    } catch (IOException e) {
      return null;
    }
  }
}
