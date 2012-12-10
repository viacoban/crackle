package org.apache.crunch.types.writable;

import org.apache.hadoop.io.BytesWritable;

public class ByteArrayWritableType extends WritableType<byte[], BytesWritable> {

  public ByteArrayWritableType() {
    super(byte[].class, BytesWritable.class, new ByteArrayInputFn(), new ByteArrayOutputFn());
  }

}
