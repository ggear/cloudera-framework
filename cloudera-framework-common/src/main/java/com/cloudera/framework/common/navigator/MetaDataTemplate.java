package com.cloudera.framework.common.navigator;

import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_GROUP;

import com.cloudera.framework.common.Driver;
import com.cloudera.nav.sdk.model.CustomIdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;

public class MetaDataTemplate extends Entity {

  public static final String DEFAULT_NAME = "a-job";

  public MetaDataTemplate(Configuration conf) {
    setName(conf.get(CONF_CLDR_JOB_GROUP, DEFAULT_NAME));
    setDescription(WordUtils.capitalize(conf.get(CONF_CLDR_JOB_GROUP, DEFAULT_NAME)
      .replace("_", " ").replace("-", " ")));
    setNamespace(Driver.METADATA_NAMESPACE);
  }

  @Override
  public String generateId() {
    return CustomIdGenerator.generateIdentity(getNamespace(), getName());
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.SDK;
  }

  @Override
  public EntityType getEntityType() {
    return EntityType.OPERATION;
  }

}
