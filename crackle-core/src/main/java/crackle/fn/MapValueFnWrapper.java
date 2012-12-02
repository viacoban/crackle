package crackle.fn;

import org.apache.crunch.fn.MapValuesFn;

public final class MapValueFnWrapper extends MapValuesFn<Object, Object, Object> {

  private final PortableFn valueFn;

  public MapValueFnWrapper(PortableFn valueFn) {
    this.valueFn = valueFn;
  }

  @Override
  public Object map(Object value) {
    return valueFn.fn().invoke(value);
  }

}
