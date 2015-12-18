package com.cloudera.framework.plugin;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Parameter;
import org.codehaus.plexus.archiver.tar.TarArchiver;
import org.codehaus.plexus.archiver.tar.TarArchiver.TarCompressionMethod;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.codehaus.plexus.util.StringUtils;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;

public class Parcel {

  public boolean isValid() throws MojoExecutionException {
    List<String> paramaters = new ArrayList<>();
    if (StringUtils.isEmpty(repositoryUrl)) {
      paramaters.add("repositoryUrl");
    }
    if (StringUtils.isEmpty(groupId)) {
      paramaters.add("groupId");
    }
    if (StringUtils.isEmpty(artifactId)) {
      paramaters.add("artifactId");
    }
    if (StringUtils.isEmpty(version)) {
      paramaters.add("version");
    }
    if (StringUtils.isEmpty(type)) {
      paramaters.add("type");
    }
    if (StringUtils.isEmpty(outputDirectory)) {
      paramaters.add("outputDirectory");
    }
    if (!paramaters.isEmpty()) {
      throw new MojoExecutionException("The required parameters " + paramaters + " are missing or invalid");
    }
    return true;
  }

  public String getArtifactName() throws MojoExecutionException {
    return isValid()
        ? artifactId + "-" + version + (StringUtils.isEmpty(classifier) ? "" : ("-" + classifier)) + "." + type : null;
  }

  public String getArtifactNamespace() throws MojoExecutionException {
    return isValid() ? groupId + ":" + artifactId + ":" + type
        + (StringUtils.isEmpty(classifier) ? "" : (":" + classifier)) + ":" + version : null;
  }

  public String getRemoteUrl() throws MojoExecutionException {
    return isValid() ? repositoryUrl + "/" + artifactId + "-" + version
        + (StringUtils.isEmpty(classifier) ? "" : ("-" + classifier)) + "." + type : null;
  }

  public String getLocalPath() throws MojoExecutionException {
    return isValid() ? "/" + groupId.replaceAll("\\.", "/") + "/" + artifactId + "/" + version + "/" + getArtifactName()
        : null;
  }

  public boolean download(Log log, String dirRepository) throws MojoExecutionException {
    boolean downloaded = false;
    GenericUrl remoteUrl = new GenericUrl(getRemoteUrl());
    GenericUrl remoteUrlSha1 = new GenericUrl(getRemoteUrl() + SUFFIX_SHA1);
    File localPath = new File(dirRepository, getLocalPath());
    File localPathSha1 = new File(dirRepository, getLocalPath() + SUFFIX_SHA1);
    if (localPath.exists() && localPathSha1.exists()) {
      if (!assertSha1(localPath, localPathSha1)) {
        localPath.delete();
        localPathSha1.delete();
      }
    } else if (localPath.exists() && !localPathSha1.exists()) {
      localPath.delete();
    } else if (!localPath.exists() && localPathSha1.exists()) {
      localPathSha1.delete();
    }
    if (!localPath.exists() && !localPathSha1.exists()) {
      log.info("Downloding: " + remoteUrl);
      if (downloaded = downloadHttpResource(remoteUrl, localPath)
          && downloadHttpResource(remoteUrlSha1, localPathSha1)) {
        if (!assertSha1(localPath, localPathSha1)) {
          localPath.delete();
          localPathSha1.delete();
          throw new MojoExecutionException(
              "Downloaded file from (" + remoteUrl + ") failed to match checksum (" + remoteUrlSha1 + ")");
        }
        log.info("Downloded: " + remoteUrl);
      }
    }
    return downloaded && localPath.exists() && localPathSha1.exists();
  }

  public boolean explode(Log log, String dirRepository, String dirBuild) throws MojoExecutionException {
    download(log, dirRepository);
    boolean exploded = false;
    File localPath = new File(dirRepository, getLocalPath());
    File explodedPath = outputDirectory.charAt(0) == '/' ? new File(outputDirectory)
        : new File(dirBuild, outputDirectory);
    try {
      FileUtils.deleteDirectory(explodedPath);
      explodedPath.mkdirs();
      TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
      unarchiver.enableLogging(new ConsoleLogger(ConsoleLogger.LEVEL_DISABLED, "Logger"));
      unarchiver.setSourceFile(localPath);
      unarchiver.setDestDirectory(explodedPath);
      unarchiver.extract();
      exploded = true;
    } catch (Exception exception) {
      throw new MojoExecutionException("Failed to explode artifact " + getArtifactNamespace() + " from (" + localPath
          + ") to (" + explodedPath + ")", exception);
    }
    return exploded;
  }

  public boolean build(Log log, String dirBuild, String dirOutput) throws MojoExecutionException {
    File buildPath = new File(dirBuild, getArtifactName());
    File buildPathSha1 = new File(dirBuild, getArtifactName() + SUFFIX_SHA1);
    File ouputPath = new File(dirOutput);
    buildPath.delete();
    buildPathSha1.delete();
    try {
      TarArchiver archiver = new TarArchiver();
      archiver.setCompression(TarCompressionMethod.gzip);
      archiver.addDirectory(ouputPath);
      archiver.setDestFile(buildPath);
      archiver.createArchive();
      FileUtils.writeStringToFile(buildPathSha1, calculateSha1(buildPath) + "\n");
      return true;
    } catch (Exception exception) {
      throw new MojoExecutionException(
          "Failed to build artifact " + getArtifactNamespace() + " from (" + ouputPath + ") to (" + buildPath + ")",
          exception);
    }
  }

  public boolean install(Log log, String dirBuild, String dirRepository) throws MojoExecutionException {
    File buildPath = new File(dirBuild, getArtifactName());
    File buildPathSha1 = new File(dirBuild, getArtifactName() + SUFFIX_SHA1);
    File repositoryPath = new File(dirRepository, getLocalPath()).getParentFile();
    try {
      FileUtils.copyFileToDirectory(buildPath, repositoryPath);
      FileUtils.copyFileToDirectory(buildPathSha1, repositoryPath);
    } catch (Exception exception) {
      throw new MojoExecutionException("Failed to install artifact " + getArtifactNamespace() + " from (" + buildPath
          + ") to (" + repositoryPath + ")", exception);
    }
    return true;
  }

  private boolean downloadHttpResource(GenericUrl remote, File local) throws MojoExecutionException {
    local.getParentFile().mkdirs();
    FileOutputStream localStream = null;
    try {
      HttpResponse httpResponse = new NetHttpTransport().createRequestFactory().buildGetRequest(remote)
          .setContentLoggingLimit(0).setConnectTimeout(5000).setReadTimeout(0).execute();
      if (httpResponse.getStatusCode() == 200) {
        httpResponse.download(localStream = new FileOutputStream(local));
        return true;
      }
    } catch (Exception exception) {
      throw new MojoExecutionException("Failed to download artifact " + getArtifactNamespace() + " from repo ("
          + repositoryUrl + ") to (" + local + ")", exception);
    } finally {
      IOUtils.closeQuietly(localStream);
    }
    return false;
  }

  private String calculateSha1(File file) throws MojoExecutionException {
    InputStream input = null;
    try {
      MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
      input = new FileInputStream(file);
      byte[] buffer = new byte[8192];
      int len = 0;
      while ((len = input.read(buffer)) != -1) {
        messageDigest.update(buffer, 0, len);
      }
      return new HexBinaryAdapter().marshal(messageDigest.digest());
    } catch (Exception exception) {
      throw new MojoExecutionException("Could not create SHA1 of file (" + file + ")");
    } finally {
      IOUtils.closeQuietly(input);
    }
  }

  private boolean assertSha1(File file, File fileSha1) throws MojoExecutionException {
    InputStream input = null;
    try {
      return IOUtils.toString(input = new FileInputStream(fileSha1)).trim().toUpperCase()
          .equals(calculateSha1(file).toUpperCase());
    } catch (Exception exception) {
      throw new MojoExecutionException("Could not load file (" + fileSha1 + ")");
    } finally {
      IOUtils.closeQuietly(input);
    }
  }

  private static final String SUFFIX_SHA1 = ".sha1";

  @Parameter(required = false, defaultValue = "http://archive.cloudera.com/cdh5/parcels/latest")
  private String repositoryUrl = "http://archive.cloudera.com/cdh5/parcels/latest";

  @Parameter(required = false, defaultValue = "com.cloudera.parcel")
  private String groupId = "com.cloudera.parcel";

  @Parameter(required = true)
  private String artifactId;

  @Parameter(required = true)
  private String version;

  @Parameter(required = false, defaultValue = "")
  private String classifier = "";

  @Parameter(required = false, defaultValue = "parcel")
  private String type = "parcel";

  @Parameter(required = false, defaultValue = "test-parcels")
  private String outputDirectory = "test-parcels";

  public Parcel() {
  }

  public Parcel(String repositoryUrl, String groupId, String artifactId, String version, String classifier, String type,
      String outputDirectory) {
    this.repositoryUrl = repositoryUrl;
    this.groupId = groupId;
    this.artifactId = artifactId;
    this.version = version;
    this.type = type;
    this.classifier = classifier;
    this.outputDirectory = outputDirectory;
  }

  public Parcel(String groupId, String artifactId, String version, String classifier, String type) {
    this.groupId = groupId;
    this.artifactId = artifactId;
    this.version = version;
    this.type = type;
    this.classifier = classifier;
  }

  public String getRepositoryUrl() {
    return repositoryUrl;
  }

  public void setRepositoryUrl(String repositoryUrl) {
    this.repositoryUrl = repositoryUrl;
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public String getArtifactId() {
    return artifactId;
  }

  public void setArtifactId(String artifactId) {
    this.artifactId = artifactId;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getClassifier() {
    return classifier;
  }

  public void setClassifier(String classifier) {
    this.classifier = classifier;
  }

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public void setOutputDirectory(String outputDirectory) {
    this.outputDirectory = outputDirectory;
  }

}
