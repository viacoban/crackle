package crackle.fn;

import clojure.lang.IFn;
import clojure.lang.Namespace;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public final class PortableFnVar implements PortableFn {

  private static final Var REQUIRE = RT.var("clojure.core", "require");

  private final Symbol nsSymbol;
  private final Symbol fnSymbol;

  private transient IFn var;

  public PortableFnVar(Namespace namespace, Symbol fnSymbol) {
    this.nsSymbol = namespace.getName();
    this.fnSymbol = fnSymbol;
  }

  @Override
  public void initialize() {
    REQUIRE.invoke(nsSymbol);
    var = RT.var(nsSymbol.getName(), fnSymbol.getName());
  }

  @Override
  public IFn getFn() {
    return var;
  }

}
