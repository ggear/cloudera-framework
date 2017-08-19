/**
 * This is an example unit test example, testing the bundled Driver
 */
#* // @formatter:off
*#
package ${package}.test;

import static com.cloudera.mytest.Driver.Name;
import static org.junit.Assert.assertEquals;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class DriverTest implements TestConstants {

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  private static final String ABS_DIR_DATASET = "/tmp/" + Name();

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(REL_DIR_DATASET).dataSetDestinationDirs(ABS_DIR_DATASET);

  @TestWith({"testMetaDataAll"})
  public void test(TestMetaData testMetaData) throws Exception {
    Driver driver = new ${package}.Driver(DfsServer.getInstance().getConf());
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(
      new String[]{DfsServer.getInstance().getPath(ABS_DIR_DATASET).toString()}));
    assertEquals(1, driver.getResults().size());
    assertEquals(100L, driver.getResults().get(0));
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
