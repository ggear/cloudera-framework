package com.cloudera.framework.common.navigator;

import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_GROUP;

import com.cloudera.nav.sdk.model.CustomIdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;

public class MetaDataTemplate extends Entity {

  public MetaDataTemplate(Configuration conf) {
    if (conf != null) {
      setName(conf.get(CONF_CLDR_JOB_GROUP));
      if (conf.get(CONF_CLDR_JOB_GROUP) != null) {
        setNamespace(conf.get(CONF_CLDR_JOB_GROUP).replace("-", "_"));
        setDescription(WordUtils.capitalize(conf.get(CONF_CLDR_JOB_GROUP).replace("-", " ")));
      }
    }
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
