package crackle.fn;

import org.apache.crunch.FilterFn;

public final class FilterFnWrapper extends FilterFn<Object> {

  private final PortableFn filterFn;
  private final PortableFnArgs args;

  public FilterFnWrapper(PortableFn filterFn, PortableFnArgs args) {
    this.filterFn = filterFn;
    this.args = args;
  }

  @Override
  public void initialize() {
    super.initialize();
    filterFn.initialize();
    args.initialize();
  }

  @Override
  public boolean accept(Object input) {
    Object result = filterFn.getFn().invoke(input, args.getArgs());
    if (result == null) {
      return false;
    }
    if (result instanceof Boolean) {
      return (Boolean) result;
    }
    return true;
  }

}
