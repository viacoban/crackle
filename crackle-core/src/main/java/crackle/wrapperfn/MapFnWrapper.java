package crackle.wrapperfn;

import org.apache.crunch.MapFn;

public final class MapFnWrapper extends MapFn<Object, Object> {

  private final PortableFn fn;

  public MapFnWrapper(PortableFn fn) {
    this.fn = fn;
  }

  @Override
  public Object map(Object input) {
    return fn.fn().invoke(input);
  }

}
