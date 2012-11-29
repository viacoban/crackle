package crackle;

import org.apache.crunch.FilterFn;

public final class FilterFnWrapper extends FilterFn<Object> {

  private final PortableFn filterFn;

  public FilterFnWrapper(PortableFn filterFn) {
    this.filterFn = filterFn;
  }

  @Override
  public boolean accept(Object input) {
    Boolean result = (Boolean) filterFn.fn().invoke(input);
    return result != null && result;
  }

}
