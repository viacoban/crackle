package crackle.fn;

import org.apache.crunch.fn.MapKeysFn;

public final class MapKeyFnWrapper extends MapKeysFn <Object, Object, Object> {

  private final PortableFn keyFn;
  private final PortableFnArgs args;

  public MapKeyFnWrapper(PortableFn keyFn, PortableFnArgs args) {
    this.keyFn = keyFn;
    this.args = args;
  }

  @Override
  public void initialize() {
    super.initialize();
    keyFn.initialize();
    args.initialize();
  }

  @Override
  public Object map(Object key) {
    return keyFn.getFn().invoke(key, args.getArgs());
  }
}
