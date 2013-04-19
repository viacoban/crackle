package crackle.types;

import org.apache.crunch.types.writable.WritableType;
import org.apache.hadoop.io.BytesWritable;

public final class ByteArrayWritableType extends WritableType<byte[], BytesWritable> {

  public ByteArrayWritableType() {
    super(byte[].class, BytesWritable.class, new ByteArrayInputFn(), new ByteArrayOutputFn());
  }

}
