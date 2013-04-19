package crackle.types;

import org.apache.crunch.MapFn;
import org.apache.hadoop.io.BytesWritable;

public final class ByteArrayOutputFn extends MapFn<byte[], BytesWritable> {

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
