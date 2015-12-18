package com.cloudera.framework.plugin.mojo;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.StringUtils;

import com.cloudera.framework.plugin.Parcel;

@Mojo(name = "deploy", requiresProject = true, defaultPhase = LifecyclePhase.DEPLOY)
public class Deploy extends AbstractMojo {

  @Parameter(defaultValue = "${project}", required = true, readonly = true)
  private MavenProject project;

  @Parameter(defaultValue = "${parcel.classifier}", required = false, readonly = true)
  private String parcelClassifier;

  @Override
  public void execute() throws MojoExecutionException {
    Parcel parcel = new Parcel(project.getGroupId(), project.getArtifactId(), project.getVersion(),
        StringUtils.isEmpty(parcelClassifier) ? "" : parcelClassifier, project.getPackaging());
    if (parcel.isValid()) {
      // TODO
    }
  }

}
