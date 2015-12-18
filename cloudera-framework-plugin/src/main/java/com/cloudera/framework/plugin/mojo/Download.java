package com.cloudera.framework.plugin.mojo;

import java.util.List;

import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import com.cloudera.framework.plugin.Parcel;

@Mojo(name = "download", requiresProject = false, defaultPhase = LifecyclePhase.VALIDATE)
public class Download extends AbstractMojo {

  @Parameter(defaultValue = "${localRepository}", required = true, readonly = true)
  private ArtifactRepository localRepository;

  @Parameter(required = true)
  private List<Parcel> parcels;

  @Override
  public void execute() throws MojoExecutionException {
    for (Parcel parcel : parcels) {
      if (parcel.isValid()) {
        parcel.download(getLog(), localRepository.getBasedir());
      }
    }
  }

}
