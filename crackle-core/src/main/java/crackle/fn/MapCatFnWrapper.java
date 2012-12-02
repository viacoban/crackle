package crackle.fn;

import clojure.lang.IPersistentCollection;
import clojure.lang.ISeq;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

public final class MapCatFnWrapper extends DoFn<Object, Object> {

  private final PortableFn fn;

  public MapCatFnWrapper(PortableFn fn) {
    this.fn = fn;
  }

  @Override
  public void process(Object input, Emitter<Object> emitter) {
    Object result = fn.fn().invoke(input);
    System.out.println(result);
    if (result instanceof IPersistentCollection) {
      ISeq values = ((IPersistentCollection) result).seq();
      while (values != null) {
        emitter.emit(values.first());
        values = values.next();
      }
    } else {
      emitter.emit(result);
    }
  }

}
