package crackle;

import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;
import java.io.Serializable;

public final class PortableFn implements Serializable {

  private static final Var REQUIRE = RT.var("clojure.core", "require");

  private final Symbol symbol;
  private transient Var var;

  public PortableFn(Symbol symbol) {
    this.symbol = symbol;
  }

  public Var var() {
    if (var == null) {
      REQUIRE.invoke(Symbol.create(symbol.getNamespace()));
      var = RT.var(symbol.getNamespace(), symbol.getName());
    }
    return var;
  }

}
