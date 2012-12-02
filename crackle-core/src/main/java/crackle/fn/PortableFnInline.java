package crackle.fn;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public final class PortableFnInline implements PortableFn {

  private static final Var REQUIRE = RT.var("clojure.core", "require");
  private static final Var EVAL = RT.var("clojure.core", "eval");

  private final String fn;
  private transient IFn fVar;

  public PortableFnInline(String fn) {
    this.fn = fn;
  }

  @Override
  public IFn fn() {
    if (fVar == null) {
      REQUIRE.invoke(Symbol.create("crackle.core"));
      fVar = (IFn) EVAL.invoke(RT.readString(fn));
    }
    return fVar;
  }

}
