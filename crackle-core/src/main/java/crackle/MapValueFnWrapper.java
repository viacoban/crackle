package crackle;

import clojure.lang.Symbol;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

public class MapValueFnWrapper extends MapFn<Pair, Pair> {

  private final PortableFn valueFn;

  public MapValueFnWrapper(Symbol valueFn) {
    this.valueFn = new PortableFn(valueFn);
  }

  @Override
  public Pair map(Pair input) {
    return new Pair<Object, Object>(input.first(), valueFn.var().invoke(input.second()));
  }

}
