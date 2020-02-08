package io.altchain.data.prototube;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

class ByteBufferInputStream extends InputStream {
  private static final int BYTE_MASK = 0xff;
  private ByteBuffer buffer;

  ByteBufferInputStream(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public int read() throws IOException {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp();
      return -1;
    } else {
      return buffer.get() & BYTE_MASK;
    }
  }

  @Override
  public int read(byte[] dest) {
    return read(dest, 0, dest.length);
  }

  @Override
  public int read(byte[] dest, int offset, int length) {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp();
      return -1;
    } else {
      int amountToGet = Math.min(buffer.remaining(), length);
      buffer.get(dest, offset, amountToGet);
      return amountToGet;
    }
  }

  @Override
  public long skip(long bytes) {
    if (buffer != null) {
      int amountToSkip = (int) Math.min(bytes, buffer.remaining());
      buffer.position(buffer.position() + amountToSkip);
      if (buffer.remaining() == 0) {
        cleanUp();
      }
      return amountToSkip;
    } else {
      return 0L;
    }
  }

  /**
   * Clean up the buffer, and potentially dispose of it using StorageUtils.dispose().
   */
  private void cleanUp() {
    if (buffer != null) {
      buffer = null;
    }
  }
}
