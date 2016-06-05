package com.cloudera.framework.testing;

import java.util.Collections;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class TestMetaData {

  public static TestMetaData getInstance() {
    return new TestMetaData();
  }

  public TestMetaData dataSetSourceDirs(String... dataSetSourceDirs) {
    this.dataSetSourceDirs = dataSetSourceDirs;
    return this;
  }

  public TestMetaData dataSetNames(String... dataSetNames) {
    this.dataSetNames = dataSetNames;
    return this;
  }

  public TestMetaData dataSetSubsets(String[][] dataSetSubsets) {
    this.dataSetSubsets = dataSetSubsets;
    return this;
  }

  public TestMetaData dataSetLabels(String[][][] dataSetLabels) {
    this.dataSetLabels = dataSetLabels;
    return this;
  }

  public TestMetaData dataSetDestinationDirs(String... dataSetDestinationDirs) {
    this.dataSetDestinationDirs = dataSetDestinationDirs;
    return this;
  }

  public TestMetaData parameters(Map... parameters) {
    this.parameters = parameters;
    return this;
  }

  public TestMetaData asserts(Map... asserts) {
    this.asserts = asserts;
    return this;
  }

  public String[] getDataSetSourceDirs() {
    return dataSetSourceDirs;
  }

  public String[] getDataSetNames() {
    return dataSetNames;
  }

  public String[][] getDataSetSubsets() {
    return dataSetSubsets;
  }

  public String[][][] getDataSetLabels() {
    return dataSetLabels;
  }

  public String[] getDataSetDestinationDirs() {
    return dataSetDestinationDirs;
  }

  public Map[] getParameters() {
    return parameters;
  }

  public Map[] getAsserts() {
    return asserts;
  }

  private String[] dataSetSourceDirs;
  private String[] dataSetNames = new String[] { null };
  private String[][] dataSetSubsets = new String[][] { { null } };
  private String[][][] dataSetLabels = new String[][][] { { { null } } };
  private String[] dataSetDestinationDirs;
  private Map[] parameters = new Map[] { Collections.EMPTY_MAP };
  private Map[] asserts = new Map[] { Collections.EMPTY_MAP };

  private TestMetaData() {
  }

}
