package com.cloudera.example;

import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.LocalClusterDfsMrTest;

/**
 * Test dataset setup
 */
@RunWith(Parameterized.class)
public class MyDatasetIngestTest extends LocalClusterDfsMrTest implements MyDataset {

  @Parameters
  public static Iterable<Object[]> paramaters() {
    return Arrays.asList(new Object[][] {
    //
    {
        //
        new String[] {
            //
            DIR_DS, DIR_DS, }, //
        new String[] {
            //
            DIR_DS_MYDATASET_TAB, DIR_DS_MYDATASET_COMMA, }, //
        new String[] {
            //
            DS_MYDATASET, DS_MYDATASET }, //
        new String[][] {
            //
            { null }, //
            { null }, //
        }, //
        new String[][][] {
            //
            { { null }, }, //
            { { null }, }, //
        } }, //
    });
  }

  public MyDatasetIngestTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
      String[][][] labels) {
    super(sources, destinations, datasets, subsets, labels);
  }

  @Test
  public void testData() throws Exception {
    Assert.assertTrue(getFileSystem().exists(new Path(getPathDfs(DIR_DS_MYDATASET_TAB))));
    Assert.assertTrue(getFileSystem().listFiles(new Path(getPathDfs(DIR_DS_MYDATASET_TAB)), true).hasNext());
    Assert.assertTrue(getFileSystem().exists(new Path(getPathDfs(DIR_DS_MYDATASET_COMMA))));
    Assert.assertTrue(getFileSystem().listFiles(new Path(getPathDfs(DIR_DS_MYDATASET_COMMA)), true).hasNext());
  }

  @Test
  public void testCleanse() throws Exception {
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        new MyDatasetCleanseDriver(getConf()).runner(new String[] { getPathDfs(DIR_DS_MYDATASET_TAB),
            getPathDfs(DIR_DS_MYDATASET_CLEANSED) }));
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        new MyDatasetCleanseDriver(getConf()).runner(new String[] { getPathDfs(DIR_DS_MYDATASET_COMMA),
            getPathDfs(DIR_DS_MYDATASET_CLEANSED) }));
  }

}
