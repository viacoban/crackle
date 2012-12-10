package org.apache.crunch.types.writable;

import org.apache.crunch.MapFn;
import org.apache.hadoop.io.BytesWritable;

public class ByteArrayOutputFn extends MapFn<byte[], BytesWritable> {

  private transient BytesWritable writable;

  @Override
  public void initialize() {
    super.initialize();
    writable = new BytesWritable();
  }

  @Override
  public BytesWritable map(byte[] input) {
    writable.set(input, 0, input.length);
    return writable;
  }

}
