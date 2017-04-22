package com.cloudera.framework.testing;

import java.util.Collections;
import java.util.Map;

/**
 * Model test meta data used in parametrised unit tests, including copying
 * datasets to DFS, unit test parametrisation and unit test expects for
 * asserts.
 * <p>
 * Usage is as so:
 * <p>
 * <pre>
 * public enum Counter {
 *   COUNTER1
 * }
 *
 * public static final TestMetaData testMetaData1 = TestMetaData.getInstance() //
 *     .dataSetSourceDirs("target/test-data", "target/test-data") //
 *     .dataSetNames("dataset-1", "dataset-1") //
 *     .dataSetSubsets(new String[][] { { "dataset-1-sub-1" }, { "dataset-1-sub-1" } }) //
 *     .dataSetLabels(new String[][][] { { { "dataset-1-sub-1-sub-1" } }, { { "dataset-1-sub-1-sub-1" } } }) //
 *     .dataSetDestinationDirs("/tmp/data/1/1", "/tmp/data/1/2") //
 *     .asserts(ImmutableMap.of(getClass().getCanonicalName(), ImmutableMap.of(Counter.COUNTER1, 0L)));
 *
 * public static final TestMetaData testMetaData2 = TestMetaData.getInstance() //
 *     .dataSetSourceDirs("target/test-data", "target/test-data") //
 *     .dataSetNames("dataset-2", "dataset-2") //
 *     .dataSetSubsets(new String[][] { { "dataset-2-sub-1" }, { "dataset-2-sub-1" } }) //
 *     .dataSetLabels(new String[][][] { { { "dataset-2-sub-1-sub-1" } }, { { "dataset-2-sub-1-sub-1" } } }) //
 *     .dataSetDestinationDirs("/tmp/data/2/1", "/tmp/data/2/2") //
 *     .asserts(ImmutableMap.of(getClass().getCanonicalName(), ImmutableMap.of(Counter.COUNTER1, 0L)));
 *
 * &#64;TestWith({ "testMetaData1", "testMetaData2" })
 * public void testCdhMetaData1(TestMetaData testMetaData) throws Exception {
 *   Assert.assertNotNull(testMetaData);
 * }
 *
 * &#64;Coercion
 * public TestMetaData toCdhMetaData(String field) {
 *   return TestRunner.toCdhMetaData(this, field);
 * }
 * </pre>
 */
@SuppressWarnings("rawtypes")
public class TestMetaData {

  private String[] dataSetSourceDirs;
  private String[] dataSetNames = new String[]{null};
  private String[][] dataSetSubsets = new String[][]{{null}};
  private String[][][] dataSetLabels = new String[][][]{{{null}}};
  private String[] dataSetDestinationDirs;
  private Map[] parameters = new Map[]{Collections.EMPTY_MAP};
  private Map[] asserts = new Map[]{Collections.EMPTY_MAP};

  private TestMetaData() {
  }

  /**
   * Get an {@link TestMetaData}
   */
  public static TestMetaData getInstance() {
    return new TestMetaData();
  }

  /**
   * Set the source directories relative to the module root
   *
   * @param dataSetSourceDirs
   */
  public TestMetaData dataSetSourceDirs(String... dataSetSourceDirs) {
    this.dataSetSourceDirs = dataSetSourceDirs;
    return this;
  }

  /**
   * Set dataset names, <code>null</code> will match all dataset directories
   *
   * @param dataSetNames
   */
  public TestMetaData dataSetNames(String... dataSetNames) {
    this.dataSetNames = dataSetNames;
    return this;
  }

  /**
   * Set the dataset subsets, <code>null</code> will match all subsets for this
   * indexed dataset
   *
   * @param dataSetSubsets
   */
  public TestMetaData dataSetSubsets(String[][] dataSetSubsets) {
    this.dataSetSubsets = dataSetSubsets;
    return this;
  }

  /**
   * Set the dataset subset labels, <code>null</code> will match all labels for
   * this indexed dataset subset
   *
   * @param dataSetLabels
   */
  public TestMetaData dataSetLabels(String[][][] dataSetLabels) {
    this.dataSetLabels = dataSetLabels;
    return this;
  }

  /**
   * Set the destination paths relative to the DFS root
   *
   * @param dataSetDestinationDirs
   */
  public TestMetaData dataSetDestinationDirs(String... dataSetDestinationDirs) {
    this.dataSetDestinationDirs = dataSetDestinationDirs;
    return this;
  }

  /**
   * Set the parameters to parametrise this test
   *
   * @param parameters
   */
  public TestMetaData parameters(Map... parameters) {
    this.parameters = parameters;
    return this;
  }

  /**
   * Set the asserts to use for {@link Assert} methods
   *
   * @param asserts
   */
  public TestMetaData asserts(Map... asserts) {
    this.asserts = asserts;
    return this;
  }

  /**
   * @return the dataSetSourceDirs
   */
  public String[] getDataSetSourceDirs() {
    return dataSetSourceDirs;
  }

  /**
   * @return the dataSetNames
   */
  public String[] getDataSetNames() {
    return dataSetNames;
  }

  /**
   * @return the dataSetSubsets
   */
  public String[][] getDataSetSubsets() {
    return dataSetSubsets;
  }

  /**
   * @return the dataSetLabels
   */
  public String[][][] getDataSetLabels() {
    return dataSetLabels;
  }

  /**
   * @return the dataSetDestinationDirs
   */
  public String[] getDataSetDestinationDirs() {
    return dataSetDestinationDirs;
  }

  /**
   * @return the parameters
   */
  public Map[] getParameters() {
    return parameters;
  }

  /**
   * @return the asserts
   */
  public Map[] getAsserts() {
    return asserts;
  }

}
