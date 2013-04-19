package crackle.types;

import carbonite.JavaBridge;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;

public final class Clojure {

  private static Kryo KRYO;

  private static final ByteArrayWritableType BYTE_ARRAY_WRITABLE_TYPE = new ByteArrayWritableType();

  private static final PType<Object> BINARY_TYPE = Writables.derived(
    Object.class, new PTypeInputFn(), new PTypeOutputFn(), BYTE_ARRAY_WRITABLE_TYPE
  );

  static {
    Writables.register(byte[].class, BYTE_ARRAY_WRITABLE_TYPE);
    try {
      JavaBridge.requireCarbonite();
      KRYO = JavaBridge.defaultRegistry();
      JavaBridge.enhanceRegistry(KRYO);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private Clojure() { }

  public static PType<Object> anything() {
    return BINARY_TYPE;
  }

  private static class PTypeInputFn extends MapFn<byte[], Object> {
    @Override
    public Object map(byte[] input) {
      return KRYO.readClassAndObject(new Input(new ByteArrayInputStream(input)));
    }
  }

  private static class PTypeOutputFn extends MapFn<Object, byte[]> {
    @Override
    public byte[] map(Object obj) {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      Output output = new Output(stream);
      KRYO.writeClassAndObject(output, obj);
      output.flush();
      return stream.toByteArray();
    }
  }
}
