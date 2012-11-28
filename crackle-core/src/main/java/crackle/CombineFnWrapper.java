package crackle;

import clojure.lang.Symbol;
import org.apache.crunch.CombineFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public final class CombineFnWrapper extends CombineFn<Object, Object> {

  private final PortableFn reduceFn;
  private final PortableFn combineFn;

  public CombineFnWrapper(Symbol reduceFn, Symbol combineFn) {
    this.reduceFn = new PortableFn(reduceFn);
    this.combineFn = new PortableFn(combineFn);
  }

  @Override
  public void process(Pair<Object, Iterable<Object>> input, Emitter<Pair<Object, Object>> emitter) {
    Object combined = reduceFn.var().invoke(combineFn.var(), input.second());
    emitter.emit(new Pair<Object, Object>(input.first(), combined));
  }
}
