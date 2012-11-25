package crackle;

import clojure.lang.RT;
import org.apache.crunch.MapFn;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BinaryTypesTest {

  @Test(dataProvider = "values")
  public void testCollectionPType(Object value) {
    MapFn<Object, Object> outputMapFn = BinaryTypes.anything().getOutputMapFn();
    MapFn<Object, Object> inputMapFn = BinaryTypes.anything().getInputMapFn();

    Object serialized = outputMapFn.map(value);
    Assert.assertNotNull(serialized);

    Assert.assertEquals(inputMapFn.map(serialized), value);
  }

  @DataProvider(name = "values")
  public Object[][] values() {
    return new Object[][] {
      new Object[] {RT.vector(1)}
    };
  }

}
