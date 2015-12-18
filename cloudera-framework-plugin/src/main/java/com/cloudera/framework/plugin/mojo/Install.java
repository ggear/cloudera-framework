package com.cloudera.framework.plugin.mojo;

import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.StringUtils;

import com.cloudera.framework.plugin.Parcel;

@Mojo(name = "install", requiresProject = true, defaultPhase = LifecyclePhase.INSTALL)
public class Install extends AbstractMojo {

  @Parameter(defaultValue = "${project}", required = true, readonly = true)
  private MavenProject project;

  @Parameter(defaultValue = "${parcel.classifier}", required = false, readonly = true)
  private String parcelClassifier;

  @Parameter(defaultValue = "${localRepository}", required = true, readonly = true)
  private ArtifactRepository localRepository;

  @Parameter(defaultValue = "${project.build.directory}", required = true, readonly = true)
  private String buildDirectory;

  @Override
  public void execute() throws MojoExecutionException {
    Parcel parcel = new Parcel(project.getGroupId(), project.getArtifactId(), project.getVersion(),
        StringUtils.isEmpty(parcelClassifier) ? "" : parcelClassifier, project.getPackaging());
    if (parcel.isValid()) {
      parcel.install(getLog(), buildDirectory, localRepository.getBasedir());
    }
  }

}
