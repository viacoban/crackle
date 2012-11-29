package crackle;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;
import org.apache.commons.lang.Validate;

public final class PortableFnSymbol implements PortableFn {

  private static final Var REQUIRE = RT.var("clojure.core", "require");

  private final Symbol symbol;
  private transient IFn var;

  public PortableFnSymbol(Symbol symbol) {
    Validate.notNull(symbol);
    Validate.notNull(symbol.getNamespace());
    Validate.notNull(symbol.getName());
    this.symbol = symbol;
  }

  @Override
  public IFn fn() {
    if (var == null) {
      REQUIRE.invoke(Symbol.create(symbol.getNamespace()));
      var = RT.var(symbol.getNamespace(), symbol.getName());
    }
    return var;
  }

}
