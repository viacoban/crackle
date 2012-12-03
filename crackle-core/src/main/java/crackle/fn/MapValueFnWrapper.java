package crackle.fn;

import org.apache.crunch.fn.MapValuesFn;

public final class MapValueFnWrapper extends MapValuesFn<Object, Object, Object> {

  private final PortableFn valueFn;
  private final PortableFnArgs args;

  public MapValueFnWrapper(PortableFn valueFn, PortableFnArgs args) {
    this.valueFn = valueFn;
    this.args = args;
  }

  @Override
  public void initialize() {
    super.initialize();
    valueFn.initialize();
    args.initialize();
  }

  @Override
  public Object map(Object value) {
    return valueFn.getFn().invoke(value, args.getArgs());
  }

}
