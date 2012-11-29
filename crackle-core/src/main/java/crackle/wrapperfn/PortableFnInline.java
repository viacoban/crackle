package crackle.wrapperfn;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Var;

public class PortableFnInline implements PortableFn {

  private static final Var EVAL = RT.var("clojure.core", "eval");

  private final String fn;
  private transient IFn fVar;

  public PortableFnInline(String fn) {
    this.fn = fn;
  }

  @Override
  public IFn fn() {
    if (fVar == null) {
      fVar = (IFn) EVAL.invoke(RT.readString(fn));
    }
    return fVar;
  }

}
