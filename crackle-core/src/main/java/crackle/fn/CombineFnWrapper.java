package crackle.fn;

import org.apache.crunch.CombineFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public final class CombineFnWrapper extends CombineFn<Object, Object> {

  private final PortableFn reduceFn;
  private final PortableFn combineFn;

  public CombineFnWrapper(PortableFn reduceFn, PortableFn combineFn) {
    this.reduceFn = reduceFn;
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
