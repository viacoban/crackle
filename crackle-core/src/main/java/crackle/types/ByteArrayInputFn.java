package crackle.types;

import org.apache.crunch.MapFn;
import org.apache.hadoop.io.BytesWritable;

public final class ByteArrayInputFn extends MapFn<BytesWritable, byte[]> {

  @Override
  public byte[] map(BytesWritable input) {
    return input.getBytes();
  }

}
