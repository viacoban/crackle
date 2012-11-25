package crackle;

import carbonite.JavaBridge;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.nio.ByteBuffer;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;

public final class BinaryTypes {

  private static Kryo KRYO;

  static {
    try {
      JavaBridge.requireCarbonite();
      KRYO = JavaBridge.defaultRegistry();
      JavaBridge.enhanceRegistry(KRYO);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static final PType<Object> SIMPLE_TYPE = Writables.derived(
    Object.class, new PTypeInputFn(), new PTypeOutputFn(), Writables.bytes()
  );

  private BinaryTypes() { }

  public static PType<Object> anything() {
    return SIMPLE_TYPE;
  }

  private static class PTypeInputFn extends MapFn<ByteBuffer, Object> {
    @Override
    public Object map(ByteBuffer input) {
      return KRYO.readClassAndObject(new Input(new ByteBufferInputStream(input)));
    }
  }

  private static class PTypeOutputFn extends MapFn<Object, ByteBuffer> {
    @Override
    public ByteBuffer map(Object obj) {
      ByteBufferOutputStream stream = new ByteBufferOutputStream(4096);
      Output output = new Output(stream);
      KRYO.writeClassAndObject(output, obj);
      output.flush();
      return stream.getByteBuffer();
    }
  }
}
