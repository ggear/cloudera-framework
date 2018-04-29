package com.cloudera.framework.common.navigator;

import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_NAME;
import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_TRANSACTION;
import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_VERSION;
import static com.cloudera.framework.common.Driver.METADATA_NAMESPACE;
import static com.cloudera.framework.common.navigator.MetaDataTemplate.DEFAULT_NAME;

import java.util.Map;
import java.util.UUID;

import com.cloudera.nav.sdk.model.CustomIdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.custom.CustomPropertyType;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.relations.RelationRole;
import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Instant;

public abstract class MetaDataExecution extends Entity {

  public static final String DEFAULT_VERSION = "1.0.0-SNAPSHOT";

  @MProperty
  private Instant started;

  @MProperty
  private Instant ended;

  @MRelation(role = RelationRole.TEMPLATE)
  private MetaDataTemplate template;

  @MProperty(register = true, fieldType = CustomPropertyType.TEXT, attribute = "Version")
  private String version;

  @MProperty(register = true, fieldType = CustomPropertyType.TEXT, attribute = "Identity")
  private String identity;

  @MProperty(register = true, fieldType = CustomPropertyType.TEXT, attribute = "Transaction")
  private String transaction;

  @MProperty(register = true, fieldType = CustomPropertyType.INTEGER, attribute = "Exit")
  private Integer exit;

  private String string;

  public MetaDataExecution() {
  }

  public MetaDataExecution(Configuration conf, MetaDataTemplate template, Integer exit) {
    setName(conf.get(CONF_CLDR_JOB_NAME, DEFAULT_NAME));
    setDescription(WordUtils.capitalize(conf.get(CONF_CLDR_JOB_NAME, DEFAULT_NAME)
      .replace("_", " ").replace("-", " ")));
    setVersion(conf.get(CONF_CLDR_JOB_VERSION, DEFAULT_VERSION));
    setTransaction(conf.get(CONF_CLDR_JOB_TRANSACTION, UUID.randomUUID().toString()));
    setNamespace(METADATA_NAMESPACE);
    setTemplate(template);
    setIdentity(CustomIdGenerator.generateIdentity(getNamespace(), getName(), getTransaction()));
    setExit(exit);
  }

  public abstract MetaDataExecution clone(MetaDataExecution metaData, Map<String, Object> metaDataMap, String string);

  public void update(MetaDataExecution metaData, Map<String, Object> metaDataMap, String string) {
    setTemplate(metaData.getTemplate());
    setName(metaDataMap.get("originalName").toString());
    setStarted(Instant.parse(metaDataMap.get("started").toString()));
    setEnded(Instant.parse(metaDataMap.get("ended").toString()));
    setVersion(((Map) ((Map) metaDataMap.get("customProperties")).get(METADATA_NAMESPACE)).get("Version").toString());
    setIdentity(((Map) ((Map) metaDataMap.get("customProperties")).get(METADATA_NAMESPACE)).get("Identity").toString());
    setTransaction(((Map) ((Map) metaDataMap.get("customProperties")).get(METADATA_NAMESPACE)).get("Transaction").toString());
    setExit(Integer.parseInt(((Map) ((Map) metaDataMap.get("customProperties")).get(METADATA_NAMESPACE)).get("Exit").toString()));
    setString(string + "/?view=detailsView&id=" + metaDataMap.get("identity").toString());
  }

  public void setString(String string) {
    this.string = string;
  }

  @Override
  public String toString() {
    return string;
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

  public void setTemplate(MetaDataTemplate template) {
    this.template = template;
  }

  public Instant getStarted() {
    return started;
  }

  public void setStarted(Instant started) {
    this.started = started;
  }

  public Instant getEnded() {
    return ended;
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

  public String getTransaction() {
    return transaction;
  }

  public void setTransaction(String transaction) {
    this.transaction = transaction;
  }

  public Integer getExit() {
    return exit;
  }

  public void setExit(Integer exit) {
    this.exit = exit;
  }

}
