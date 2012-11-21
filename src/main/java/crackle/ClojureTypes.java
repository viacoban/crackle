package crackle;

import carbonite.JavaBridge;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.nio.ByteBuffer;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;

public final class ClojureTypes {

  private static Kryo KRYO;

  static {
    try {
      KRYO = JavaBridge.defaultRegistry();
      JavaBridge.enhanceRegistry(KRYO);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static final MapFn<ByteBuffer, Object> INPUT_FN = new MapFn<ByteBuffer, Object>() {
    @Override
    public Object map(ByteBuffer input) {
      return KRYO.readClassAndObject(new Input(new ByteBufferInputStream(input)));
    }
  };

  private static final MapFn<Object, ByteBuffer> OUTPUT_FN = new MapFn<Object, ByteBuffer>() {
    @Override
    public ByteBuffer map(Object output) {
      ByteBufferOutputStream stream = new ByteBufferOutputStream();
      KRYO.writeClassAndObject(new Output(stream), output);
      return stream.getByteBuffer();
    }
  };

  private static final PType<Object> SIMPLE_TYPE =
    Writables.derived(Object.class, INPUT_FN, OUTPUT_FN, Writables.bytes());

  private static final PTableType<Object, Object> TABLE_TYPE =
    Writables.tableOf(SIMPLE_TYPE, SIMPLE_TYPE);

  private ClojureTypes() { }

  public static PType<Object> getSimpleType() {
    return SIMPLE_TYPE;
  }

  public static PTableType<Object, Object> getTableType() {
    return TABLE_TYPE;
  }
}
