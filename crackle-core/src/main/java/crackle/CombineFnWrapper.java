package crackle;

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
  public void process(Pair<Object, Iterable<Object>> input, Emitter<Pair<Object, Object>> emitter) {
    Object combined = reduceFn.fn().invoke(combineFn.fn(), input.second());
    emitter.emit(new Pair<Object, Object>(input.first(), combined));
  }
}
