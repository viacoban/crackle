package crackle.fn;

import java.io.Serializable;

public final class PortableFnArgs implements Serializable {

  private final Object args;

  private PortableFnArgs(Object args) {
    this.args = args;
  }

  public void initialize() {
    //todo: deserialize
  }

  public Object getArgs() {
    return args;
  }

  public static PortableFnArgs getInstance(Object args) {
    //todo: serialize
    return new PortableFnArgs(args);
  }
}
