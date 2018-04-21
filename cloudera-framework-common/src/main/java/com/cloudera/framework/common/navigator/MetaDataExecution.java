package com.cloudera.framework.common.navigator;

import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_GROUP;
import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_NAME;

import java.util.UUID;

import com.cloudera.nav.sdk.model.CustomIdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.custom.CustomPropertyType;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.relations.RelationRole;
import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Instant;

public class MetaDataExecution extends Entity {

  @MRelation(role = RelationRole.TEMPLATE)
  private MetaDataTemplate template;

  @MProperty
  private Instant started;

  @MProperty
  private Instant ended;

  @MProperty(register = true, fieldType = CustomPropertyType.TEXT)
  private String version;

  public MetaDataExecution(Configuration conf, MetaDataTemplate template, String version) {
    if (conf != null) {
      setName(conf.get(CONF_CLDR_JOB_NAME));
      if (conf.get(CONF_CLDR_JOB_GROUP) != null) {
        setNamespace(conf.get(CONF_CLDR_JOB_GROUP).replace("-", "_"));
        setDescription(WordUtils.capitalize(conf.get(CONF_CLDR_JOB_GROUP).replace("-", " ")));
      }
    }
    setTemplate(template);
    this.version = version;
    started = ended = Instant.now();
  }

  public String generateId() {
    return CustomIdGenerator.generateIdentity(getNamespace(), getName(), UUID.randomUUID().toString());
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.SDK;
  }

  @Override
  public EntityType getEntityType() {
    return EntityType.OPERATION_EXECUTION;
  }

  public MetaDataTemplate getTemplate() {
    return template;
  }

  public Instant getStarted() {
    return started;
  }

  public Instant getEnded() {
    return ended;
  }

  public void setTemplate(MetaDataTemplate template) {
    this.template = template;
  }

  public void setStarted(Instant started) {
    this.started = started;
  }

  public void setEnded(Instant ended) {
    this.ended = ended;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

}
