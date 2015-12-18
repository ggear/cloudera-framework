package com.cloudera.framework.plugin.mojo;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.StringUtils;

import com.cloudera.framework.plugin.Parcel;

@Mojo(name = "build", requiresProject = true,  defaultPhase = LifecyclePhase.PACKAGE)
public class Build extends AbstractMojo {

  @Parameter(defaultValue = "${project}", required = true, readonly = true)
  private MavenProject project;

  @Parameter(defaultValue = "${parcel.classifier}", required = false, readonly = true)
  private String parcelClassifier;

  @Parameter(defaultValue = "${project.build.directory}", required = true, readonly = true)
  private String buildDirectory;

  @Parameter(defaultValue = "${project.build.outputDirectory}", required = true, readonly = true)
  private String outputDirectory;

  @Override
  public void execute() throws MojoExecutionException {
    Parcel parcel = new Parcel(project.getGroupId(), project.getArtifactId(), project.getVersion(),
        StringUtils.isEmpty(parcelClassifier) ? "" : parcelClassifier, project.getPackaging());
    if (parcel.isValid()) {
      parcel.build(getLog(), buildDirectory, outputDirectory);
    }
  }

}
