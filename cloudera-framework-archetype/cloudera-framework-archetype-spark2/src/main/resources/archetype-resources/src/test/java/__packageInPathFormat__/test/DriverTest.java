/**
 * This is a unit test example, testing the bundled {@link ${package}.Driver}
 * <p>
 * More extensive examples are bundled with the cloudera-framework here:
 * https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example
 */
#* // @formatter:off
*#
package ${package}.test;

import ${package}.Driver;
import static ${package}.Driver.Name;
import static ${package}.Driver.PathInput;
import static ${package}.Driver.PathOutput;

import static com.cloudera.framework.common.Driver.Counter.FILES_OUT;
import static com.cloudera.framework.common.Driver.Counter.RECORDS_IN;
import static com.cloudera.framework.common.Driver.Counter.RECORDS_OUT;
import static com.cloudera.framework.common.Driver.FAILURE_ARGUMENTS;
import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static org.junit.Assert.assertEquals;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.google.common.collect.ImmutableMap;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * The test class, {@link org.junit.runners.Suite Suite} classes are also supported
 */
@RunWith(TestRunner.class) // This is required to instantiate any CdhServers, a runtime exception will result otherwise
public class DriverTest implements TestConstants {

  @ClassRule // Instantiate a class lifecycle bound DFS environment
  // Note that the DfsServer.Runtime.LOCAL_FS profile is selected by default, DfsServer.Runtime.CLUSTER_DFS is also available
  public static final DfsServer dfs = DfsServer.getInstance();

  @ClassRule // Instantiate a class lifecycle bound Spark environment
  // Note that if a DfsServer was not specified, one would be instantiated implicitly as part of the SparkServer dependencies
  public static final SparkServer spark = SparkServer.getInstance();

  /**
   * Test the driver without required argumnets
   */
  @Test
  public void testFailure() throws Exception {
    assertEquals(FAILURE_ARGUMENTS, new Driver(null).runner());
    assertEquals(0, dfs.listFilesDfs(DATASET_DIR_OUTPUT).length);
  }

  // Test meta data, specifying all datasets, subsets, labels and expected counter asserts
  public final TestMetaData testMetaDataAll = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetDestinationDirs(DATASET_DIR_INPUT)
    .asserts(ImmutableMap.of(Driver.class.getName(), ImmutableMap.of(
      FILES_OUT, 3,
      RECORDS_IN, 103L,
      RECORDS_OUT, 100L
    )));

  // Test meta data, specifying the CSV, pistine dataset and expected counter asserts
  public final TestMetaData testMetaDataPristine = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames(Name()).dataSetSubsets(new String[][]{{"csv"}}).dataSetLabels(new String[][][]{{{"pristine"}}})
    .dataSetDestinationDirs(DATASET_DIR_INPUT).asserts(ImmutableMap.of(Driver.class.getName(), ImmutableMap.of(
      FILES_OUT, 3,
      RECORDS_IN, 100L,
      RECORDS_OUT, 100L
    )));

  // Test meta data, specifying the CSV, corrupt dataset and expected counter asserts
  public final TestMetaData testMetaDataCorrupt = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames(Name()).dataSetSubsets(new String[][]{{"csv"}}).dataSetLabels(new String[][][]{{{"corrupt"}}})
    .dataSetDestinationDirs(DATASET_DIR_INPUT).asserts(ImmutableMap.of(Driver.class.getName(), ImmutableMap.of(
      FILES_OUT, 1,
      RECORDS_IN, 3L,
      RECORDS_OUT, 0L
    )));

  /**
   * Test the driver with {@link #testMetaDataAll}, {@link #testMetaDataPristine} and {@link #testMetaDataCorrupt}
   */
  @TestWith({"testMetaDataAll", "testMetaDataPristine", "testMetaDataCorrupt"})
  public void testSuccess(TestMetaData testMetaData) throws Exception {

    // Construct the driver
    Driver driver = new Driver(dfs.getConf());

    // Execute the driver, asserting a successful return
    assertEquals(SUCCESS, driver.runner(dfs.getPath(DATASET_DIR).toString()));

    // Assert the expected counters are collated in the driver
    assertCounterEquals(testMetaData, driver.getCounters());

    // Assert the expected partitions were created within HDFS
    assertCounterEquals(testMetaData, Driver.class.getName(), FILES_OUT,
      dfs.listFilesDfs(DATASET_DIR_OUTPUT).length);

  }

  // Dataset directories
  private static final String DATASET_DIR = "/tmp/" + Name();
  private static final String DATASET_DIR_INPUT = DATASET_DIR + "/" + PathInput();
  private static final String DATASET_DIR_OUTPUT = DATASET_DIR + "/" + PathOutput();

  /**
   * Required by the test harness to reflect from a class field name to a {@link TestMetaData} instance
   */
  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
