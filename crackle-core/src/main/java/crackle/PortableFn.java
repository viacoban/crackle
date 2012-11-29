package crackle;

import clojure.lang.IFn;
import java.io.Serializable;

public interface PortableFn extends Serializable {

  IFn fn();

}
