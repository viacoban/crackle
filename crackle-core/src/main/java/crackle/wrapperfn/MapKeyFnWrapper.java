package crackle.wrapperfn;

import org.apache.crunch.fn.MapKeysFn;

public final class MapKeyFnWrapper extends MapKeysFn <Object, Object, Object> {

  private final PortableFn keyFn;

  public MapKeyFnWrapper(PortableFn keyFn) {
    this.keyFn = keyFn;
  }

  @Override
  public Object map(Object key) {
    return keyFn.fn().invoke(key);
  }
}
