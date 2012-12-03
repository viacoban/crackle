package crackle.fn;

import org.apache.crunch.MapFn;

public final class MapFnWrapper extends MapFn<Object, Object> {

  private final PortableFn fn;
  private final PortableFnArgs args;

  public MapFnWrapper(PortableFn fn, PortableFnArgs args) {
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
  public Object map(Object input) {
    return fn.getFn().invoke(input, args.getArgs());
  }

}
