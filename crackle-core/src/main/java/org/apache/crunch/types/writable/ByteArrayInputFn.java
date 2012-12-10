package org.apache.crunch.types.writable;

import org.apache.crunch.MapFn;
import org.apache.hadoop.io.BytesWritable;

public class ByteArrayInputFn extends MapFn<BytesWritable, byte[]> {

  @Override
  public byte[] map(BytesWritable input) {
    return input.getBytes();
  }

}
