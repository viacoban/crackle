package crackle.wrapperfn;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

public class MapValueFnWrapper extends MapFn<Pair, Pair> {

  private final PortableFn valueFn;

  public MapValueFnWrapper(PortableFn valueFn) {
    this.valueFn = valueFn;
  }

  @Override
  public Pair map(Pair input) {
    return new Pair<Object, Object>(input.first(), valueFn.fn().invoke(input.second()));
  }

}
