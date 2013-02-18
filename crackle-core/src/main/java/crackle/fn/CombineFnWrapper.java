package crackle.fn;

import clojure.lang.Namespace;
import clojure.lang.Symbol;
import org.apache.crunch.CombineFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public final class CombineFnWrapper extends CombineFn<Object, Object> {

  private final PortableFn reduceFn;
  private final PortableFn combineFn;

  public CombineFnWrapper(PortableFn combineFn, PortableFnArgs args) {
    this.reduceFn = new PortableFnVar(
      Namespace.find(Symbol.create("clojure.core")),
      Symbol.create("clojure.core", "reduce")
    );
    this.combineFn = combineFn;
  }

  @Override
  public void initialize() {
    super.initialize();
    reduceFn.initialize();
    combineFn.initialize();
  }

  @Override
  public void process(Pair<Object, Iterable<Object>> input, Emitter<Pair<Object, Object>> emitter) {
    Object combined = reduceFn.getFn().invoke(combineFn.getFn(), input.second());
    emitter.emit(new Pair<Object, Object>(input.first(), combined));
  }
}
