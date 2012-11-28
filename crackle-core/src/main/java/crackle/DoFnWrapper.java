package crackle;

import clojure.lang.Symbol;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

public final class DoFnWrapper extends DoFn<Object, Object> {

  private final PortableFn fn;
  private final PortableFn emitterFn;

  public DoFnWrapper(Symbol emitterFn, Symbol fn) {
    this.fn = new PortableFn(fn);
    this.emitterFn = new PortableFn(emitterFn);
  }

  @Override
  public void process(Object input, Emitter emitter) {
    fn.var().invoke(emitterFn.var().invoke(emitter), input);
  }

}
