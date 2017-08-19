/**
 * This is a unit test example, testing the bundled {@link ${package}.Driver}
 * <p>
 * More detailed examples are available here:
 * https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example
 */
#* // @formatter:off
*#
package ${package}.test;

import ${package}.Driver;

import static com.cloudera.framework.common.Driver.Counter.FILES_OUT;
import static com.cloudera.framework.common.Driver.Counter.RECORDS_IN;
import static com.cloudera.framework.common.Driver.Counter.RECORDS_OUT;
import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static com.cloudera.mytest.Driver.Name;
import static com.cloudera.mytest.Driver.PathInput;
import static com.cloudera.mytest.Driver.PathOutput;
import static org.junit.Assert.assertEquals;

import com.cloudera.framework.common.Driver.Counter;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.cloudera.mytest.Driver;
import com.google.common.collect.ImmutableMap;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Note that the {@link TestRunner} is required to run the unit tests, attempting to instantiate any
 * {@link com.cloudera.framework.testing.server.CdhServer CdhServers} without it will result in a runtime error
 */
@RunWith(TestRunner.class)
public class DriverTest implements TestConstants {

  // Instantiate a class lifecycle bound DFS environment
  // Note that the DfsServer.Runtime.LOCAL_FS profile is selected by default, DfsServer.Runtime.CLUSTER_DFS is also available
  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  // Instantiate a class lifecycle bound Spark environment
  // Note that if a DfsServer was not specififed, one would be instantiated implicitly as part of the SparkServer dependencies
  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  // Dataset directories
  private static final String DFS_DATASET = "/tmp/" + Name();
  private static final String DFS_DATASET_INPUT = DFS_DATASET + "/" + PathInput();
  private static final String DFS_DATASET_OUTPUT = DFS_DATASET + "/" + PathOutput();

  // The test meta data, picking up all datasets, subsets, labels and specifying expected counter asserts
  public final TestMetaData testMetaDataAll = TestMetaData.getInstance()
    .dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetDestinationDirs(DFS_DATASET_INPUT)
    .asserts(ImmutableMap.of(Driver.class.getName(), ImmutableMap.of(
      FILES_OUT, 3,
      RECORDS_IN, 100L,
      RECORDS_OUT, 100L
    )));

  /**
   * Test the driver against all the bundled test datasets
   */
  @TestWith({"testMetaDataAll"})
  public void test(TestMetaData testMetaData) throws Exception {

    // Construct the driver
    Driver driver = new Driver(dfsServer.getConf());

    // Execute the driver, asserting a successful return
    assertEquals(SUCCESS, driver.runner(dfsServer.getPath(DFS_DATASET).toString()));

    // Assert the expected counters are collated in the driver
    assertCounterEquals(testMetaData, driver.getCounters());

    // Assert the expected partitions were created within HDFS
    assertCounterEquals(testMetaData, Driver.class.getName(), FILES_OUT, dfsServer.listFilesDfs(DFS_DATASET_OUTPUT).length);

  }

  /**
   * Required by the test harness to reflect from a class field name to a {@link TestMetaData} instance
   */
  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
