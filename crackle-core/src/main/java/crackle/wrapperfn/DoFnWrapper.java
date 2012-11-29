package crackle.wrapperfn;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

public final class DoFnWrapper extends DoFn<Object, Object> {

  private final PortableFn fn;
  private final PortableFn emitterFn;

  public DoFnWrapper(PortableFn fn, PortableFn emitterFn) {
    this.fn = fn;
    this.emitterFn = emitterFn;
  }

  @Override
  public void process(Object input, Emitter emitter) {
    fn.fn().invoke(emitterFn.fn().invoke(emitter), input);
  }

}
