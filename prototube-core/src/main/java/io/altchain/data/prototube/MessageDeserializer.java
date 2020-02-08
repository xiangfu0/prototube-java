package io.altchain.data.prototube;

import com.google.protobuf.AbstractMessage;
import io.altchain.data.idl.Prototube;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class MessageDeserializer implements Serializable {
  public static final byte[] HEADER = "PBTB".getBytes(StandardCharsets.UTF_8);
  private static final long serialVersionUID = 0L;
  protected transient Prototube.PrototubeMessageHeader.Builder header;
  protected transient AbstractMessage.Builder<?> record;
  private final String payloadClassName;

  protected MessageDeserializer(String payloadClassName) throws
      NoSuchMethodException, InvocationTargetException,
      IllegalAccessException, ClassNotFoundException {
    this.payloadClassName = payloadClassName;
    this.header = Prototube.PrototubeMessageHeader.newBuilder();
    Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(payloadClassName);
    this.record = (AbstractMessage.Builder) clazz.getMethod("newBuilder").invoke(null);
  }

  protected MessageDeserializer(
          String payloadClassName,
          Prototube.PrototubeMessageHeader.Builder header,
          AbstractMessage.Builder<?> record) {
    this.payloadClassName = payloadClassName;
    this.header = header;
    this.record = record;
  }

  private void readObject(ObjectInputStream in)
          throws IOException, ClassNotFoundException, NoSuchMethodException,
          InvocationTargetException, IllegalAccessException {
    in.defaultReadObject();
    this.header = Prototube.PrototubeMessageHeader.newBuilder();
    Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(payloadClassName);
    this.record = (AbstractMessage.Builder) clazz.getMethod("newBuilder").invoke(null);
  }

  protected boolean deserializeMessage(ByteBuffer buf) throws IOException {
    final byte[] magicNumbers = new byte[HEADER.length];
    try (InputStream is = new ByteBufferInputStream(buf)) {
      int ret = is.read(magicNumbers);
      if (ret != HEADER.length || !Arrays.equals(magicNumbers, HEADER)) {
        return false;
      }
      header.clear().mergeDelimitedFrom(is);
      record.clear().mergeDelimitedFrom(is);
    }
    return true;
  }
}
