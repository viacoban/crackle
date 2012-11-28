package crackle;

import clojure.lang.Symbol;
import org.apache.crunch.MapFn;

public final class MapFnWrapper extends MapFn<Object, Object> {

  private final PortableFn fn;

  public MapFnWrapper(Symbol symbol) {
    fn = new PortableFn(symbol);
  }

  @Override
  public Object map(Object input) {
    return fn.var().invoke(input);
  }

}
