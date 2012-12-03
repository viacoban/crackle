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
    Boolean result = (Boolean) filterFn.getFn().invoke(input, args.getArgs());
    return result != null && result;
  }

}
