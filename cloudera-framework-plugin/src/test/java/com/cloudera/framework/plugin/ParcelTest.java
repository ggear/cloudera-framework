package com.cloudera.framework.plugin;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.SystemStreamLog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ParcelTest {

  private static final String PARCEL_TYPE = "parcel";
  private static final String PARCEL_CLASSIFIER = "el6";
  private static final String PARCEL_VERSION = "1.3c5";
  private static final String PARCEL_ARTIFACT_ID = "SQOOP_NETEZZA_CONNECTOR";
  private static final String PARCEL_GROUP_ID = "com.cloudera.parcel";
  private static final String PARCEL_OUTPUT_DIR = "test-parcels";
  private static final String PARCEL_REPO_URL = "http://archive.cloudera.com/sqoop-connectors/parcels/1.4";

  private static final String PATH_BUILD = new File(".").getAbsolutePath() + "/target";
  private static final String PATH_MAVEN_REPO = new File(".").getAbsolutePath() + "/target/local-repo-unit-test";

  @Test(expected = MojoExecutionException.class)
  public void testIsValidDefaults() throws MojoExecutionException {
    Assert.assertFalse(new Parcel().isValid());
  }

  @Test(expected = MojoExecutionException.class)
  public void testIsValidEmpty() throws MojoExecutionException {
    Assert.assertTrue(new Parcel("", "", "", "", "", "", "").isValid());
  }

  @Test()
  public void testIsValid() throws MojoExecutionException {
    Assert.assertTrue(new Parcel("a", "a", "a", "a", "a", "a", "a").isValid());
  }

  @Test
  public void testGetArtifactNames() throws MojoExecutionException {
    Assert.assertEquals(PARCEL_ARTIFACT_ID + "-" + PARCEL_VERSION + "-" + PARCEL_CLASSIFIER + "." + PARCEL_TYPE,
        new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION, PARCEL_CLASSIFIER, PARCEL_TYPE,
            PARCEL_OUTPUT_DIR).getArtifactName());
  }

  @Test
  public void testGetArtifactNamespace() throws MojoExecutionException {
    Assert.assertEquals(
        PARCEL_GROUP_ID + ":" + PARCEL_ARTIFACT_ID + ":" + PARCEL_TYPE + ":" + PARCEL_CLASSIFIER + ":" + PARCEL_VERSION,
        new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION, PARCEL_CLASSIFIER, PARCEL_TYPE,
            PARCEL_OUTPUT_DIR).getArtifactNamespace());
  }

  @Test
  public void testGetRemoteUrl() throws MojoExecutionException {
    Assert.assertEquals(
        PARCEL_REPO_URL + "/" + PARCEL_ARTIFACT_ID + "-" + PARCEL_VERSION + "-" + PARCEL_CLASSIFIER + "." + PARCEL_TYPE,
        new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION, PARCEL_CLASSIFIER, PARCEL_TYPE,
            PARCEL_OUTPUT_DIR).getRemoteUrl());
  }

  @Test
  public void testGetLocalPath() throws MojoExecutionException {
    Assert.assertEquals(
        "/" + PARCEL_GROUP_ID.replaceAll("\\.", "/") + "/" + PARCEL_ARTIFACT_ID + "/" + PARCEL_VERSION + "/"
            + PARCEL_ARTIFACT_ID + "-" + PARCEL_VERSION + "-" + PARCEL_CLASSIFIER + "." + PARCEL_TYPE,
        new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION, PARCEL_CLASSIFIER, PARCEL_TYPE,
            PARCEL_OUTPUT_DIR).getLocalPath());
  }

  @Test(expected = MojoExecutionException.class)
  public void testDownloadSquattingHost() throws MojoExecutionException, IOException {
    Assert.assertFalse(new Parcel("http://some.non.existant.host.com/sqoop-connectors/parcels/1.4", PARCEL_GROUP_ID,
        PARCEL_ARTIFACT_ID, PARCEL_VERSION, PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR)
            .download(new SystemStreamLog(), PATH_MAVEN_REPO));
  }

  @Test(expected = MojoExecutionException.class)
  public void testDownloadBadHost() throws MojoExecutionException, IOException {
    Assert.assertFalse(new Parcel("http://KHAsdalj123lljasd/sqoop-connectors/parcels/1.4", PARCEL_GROUP_ID,
        PARCEL_ARTIFACT_ID, PARCEL_VERSION, PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR)
            .download(new SystemStreamLog(), PATH_MAVEN_REPO));
  }

  @Test(expected = MojoExecutionException.class)
  public void testDownloadBadPath() throws MojoExecutionException, IOException {
    Assert
        .assertFalse(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, "1.3c5.some.nonexistant.version",
            PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).download(new SystemStreamLog(), PATH_MAVEN_REPO));
  }

  @Test
  @SuppressWarnings("resource")
  public void testDownload() throws MojoExecutionException, IOException {
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).download(new SystemStreamLog(), PATH_MAVEN_REPO));
    Assert.assertFalse(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).download(new SystemStreamLog(), PATH_MAVEN_REPO));
    FileUtils.listFiles(new File(PATH_MAVEN_REPO), null, true).iterator().next().delete();
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).download(new SystemStreamLog(), PATH_MAVEN_REPO));
    Assert.assertFalse(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).download(new SystemStreamLog(), PATH_MAVEN_REPO));
    FileUtils.listFiles(new File(PATH_MAVEN_REPO), null, true).toArray(new File[2])[1].delete();
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).download(new SystemStreamLog(), PATH_MAVEN_REPO));
    Assert.assertFalse(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).download(new SystemStreamLog(), PATH_MAVEN_REPO));
    new FileOutputStream(FileUtils.listFiles(new File(PATH_MAVEN_REPO), null, true).iterator().next(), true)
        .getChannel().truncate(0).close();
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).download(new SystemStreamLog(), PATH_MAVEN_REPO));
    Assert.assertFalse(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).download(new SystemStreamLog(), PATH_MAVEN_REPO));
    new FileOutputStream(FileUtils.listFiles(new File(PATH_MAVEN_REPO), null, true).toArray(new File[2])[1], true)
        .getChannel().truncate(0).close();
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).download(new SystemStreamLog(), PATH_MAVEN_REPO));
    Assert.assertFalse(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).download(new SystemStreamLog(), PATH_MAVEN_REPO));
  }

  @Test(expected = MojoExecutionException.class)
  public void testExplodeSquattingHost() throws MojoExecutionException, IOException {
    Assert.assertFalse(new Parcel("http://some.non.existant.host.com/sqoop-connectors/parcels/1.4", PARCEL_GROUP_ID,
        PARCEL_ARTIFACT_ID, PARCEL_VERSION, PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR)
            .explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
  }

  @Test(expected = MojoExecutionException.class)
  public void testExplodeBadHost() throws MojoExecutionException, IOException {
    Assert.assertFalse(new Parcel("http://KHAsdalj123lljasd/sqoop-connectors/parcels/1.4", PARCEL_GROUP_ID,
        PARCEL_ARTIFACT_ID, PARCEL_VERSION, PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR)
            .explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
  }

  @Test(expected = MojoExecutionException.class)
  public void testExplodeBadPath() throws MojoExecutionException, IOException {
    Assert.assertFalse(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID,
        "1.3c5.some.nonexistant.version", PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR)
            .explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
  }

  @Test
  @SuppressWarnings("resource")
  public void testExplode() throws MojoExecutionException, IOException {
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
    FileUtils.listFiles(new File(PATH_MAVEN_REPO), null, true).iterator().next().delete();
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
    FileUtils.listFiles(new File(PATH_MAVEN_REPO), null, true).toArray(new File[2])[1].delete();
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
    new FileOutputStream(FileUtils.listFiles(new File(PATH_MAVEN_REPO), null, true).iterator().next(), true)
        .getChannel().truncate(0).close();
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
    new FileOutputStream(FileUtils.listFiles(new File(PATH_MAVEN_REPO), null, true).toArray(new File[2])[1], true)
        .getChannel().truncate(0).close();
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
    Assert.assertTrue(new Parcel(PARCEL_REPO_URL, PARCEL_GROUP_ID, PARCEL_ARTIFACT_ID, PARCEL_VERSION,
        PARCEL_CLASSIFIER, PARCEL_TYPE, PARCEL_OUTPUT_DIR).explode(new SystemStreamLog(), PATH_MAVEN_REPO, PATH_BUILD));
  }

  @Before
  public void setUpTestMavenRepo() throws IOException {
    File pathMavenRepo = new File(PATH_MAVEN_REPO);
    FileUtils.deleteDirectory(pathMavenRepo);
    pathMavenRepo.mkdirs();
    File pathBuild = new File(PATH_BUILD, PARCEL_OUTPUT_DIR);
    FileUtils.deleteDirectory(pathBuild);
    pathBuild.mkdirs();
  }

}
