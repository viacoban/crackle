package crackle.fn;

import clojure.lang.IPersistentCollection;
import clojure.lang.ISeq;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

public final class MapCatFnWrapper extends DoFn<Object, Object> {

  private final PortableFn fn;
  private final PortableFnArgs args;

  public MapCatFnWrapper(PortableFn fn, PortableFnArgs args) {
    this.fn = fn;
    this.args = args;
  }

  @Override
  public void initialize() {
    super.initialize();
    fn.initialize();
    args.initialize();
  }

  @Override
  public void process(Object input, Emitter<Object> emitter) {
    Object result = fn.getFn().invoke(input, args.getArgs());
    if (result == null) {
      return;
    }

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
