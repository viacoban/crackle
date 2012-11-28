package crackle;

import clojure.lang.Symbol;
import org.apache.crunch.FilterFn;

public final class FilterFnWrapper extends FilterFn<Object> {

  private final PortableFn filterFn;

  public FilterFnWrapper(Symbol fn) {
    this.filterFn = new PortableFn(fn);
  }

  @Override
  public boolean accept(Object input) {
    Boolean result = (Boolean) filterFn.var().invoke(input);
    return result != null && result;
  }

}
