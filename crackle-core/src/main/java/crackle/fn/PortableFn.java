package crackle.fn;

import clojure.lang.IFn;
import java.io.Serializable;

public interface PortableFn extends Serializable {

  void initialize();

  IFn getFn();

}
