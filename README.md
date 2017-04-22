# Cloudera Framework

Provide an example organisation wide Cloudera (i.e. Hadoop ecosystem) project framework, 
defining corporate standards on runtime components, datasets, libraries, testing and project 
structure to facilitate operating with a continuous deployment pipeline. This example includes 
[client](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-library/cloudera-framework-library-client)/[runtime](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-library/cloudera-framework-library-runtime)/[thirdparty](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-library/cloudera-framework-library-thirdparty)
bill-of-materials,
[utlitiy](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-common/src/main/java/com/cloudera/framework/common)/[driver](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-common/src/main/java/com/cloudera/framework/common/Driver.java)
libraries and a unit test [harness](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-testing/src/main/java/com/cloudera/framework/testing)
with [examples](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-testing/src/test/java/com/cloudera/framework/testing/server),
providing full coverage against CDH:

* HDFS
* YARN
* Kudu
* Impala
* Spark
* MR2
* Hive
* Flume
* Kafka

An [example](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-example) 
is included, codifying the standards, providing an end to end data 
[streaming](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-example/src/main/java/com/cloudera/framework/example/stream), [ingest](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-example/src/main/java/com/cloudera/framework/example/ingest), [modeling](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-example/src/main/java/com/cloudera/framework/example/model), [testing](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-example/src/test/java/com/cloudera/framework/example)
pipeline, with synthetic [datasets](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-example/src/test/resources/data/mydataset) 
to exercise the codebase.

Finally, an [archetype](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype) 
is included to provide a bare bones starter client module.

## Requirements

To compile, build and package from source, this project requires:

* Java 8
* Scala 2.10
* Python 2.7
* Maven 3

To run the unit and integrations tests, binaries and meta-data are provided for all CDH components:

* MacOS 10.12.x
* CentOS/RHEL 6.x
* CentOS/RHEL 7.x
* Ubuntu LTS 14.04.x

Some CDH components have additional coverage on other platforms and versions.

Note that in addition to Maven dependencies, Cloudera 
[parcels](http://www.cloudera.com/documentation/enterprise/latest/topics/cm_ig_parcels.html) 
are used to manage platform dependent binaries. This project depends on the 
[cloudera-parcel-plugin](https://github.com/ggear/cloudera-parcel/tree/master/cloudera-parcel-plugin) 
to help manage its parcel dependencies.

## Install

This project can be installed to a local repository as per:

```bash
git clone git@github.com:ggear/cloudera-framework.git
cd cloudera-framework
mvn install
```

Alternatively, the module can be included as a binary dependency in maven, for example pulling in the 
core client bill-of-materials and test harness can be achieved as so:

```xml
  <project>
    <repositories>
      <repository>
        <id>cloudera-framework</id>
        <url>http://52.63.86.162:80/artifactory/cloudera-framework-releases</url>
        <snapshots>
          <enabled>false</enabled>
          <updatePolicy>never</updatePolicy>
        </snapshots>
      </repository>
    </repositories>
    <dependencies>
      <dependency>
        <groupId>com.cloudera.framework.library.client</groupId>
        <artifactId>cloudera-framework-library-client-core</artifactId>
        <version>1.5.2-cdh5.11.0</version>
        <type>pom</type>
        <scope>provided</scope>
      </dependency>
      <dependency>
        <groupId>com.cloudera.framework</groupId>
        <artifactId>cloudera-framework-testing</artifactId>
        <version>1.5.2-cdh5.11.0</version>
        <scope>test</scope>
      </dependency>
    </dependencies>
	</project>
```

## Usage

The cloudera-framework includes an 
[example](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-example) 
module which exercises the entire codebase. In addition, an archetype is available allowing one to stand 
up a simple, bare bones project for new cloudera-framework clients as so (assuming Eclipse as the IDE):

```bash
# Change the following variables to appropriate values for the target environment
export CF_VERSION=1.5.2
export CDH_VERSION=5.11.0
export ECLIPSE_WORKSPACE=/tmp/eclipse/workspace
mvn archetype:generate -B \
  -DarchetypeRepository=http://52.63.86.162/artifactory/cloudera-framework-releases \
  -DarchetypeGroupId=com.cloudera.framework \
  -DarchetypeArtifactId=cloudera-framework-archetype \
  -DarchetypeVersion=$CF_VERSION-cdh$CDH_VERSION \
  -DgroupId=com.cloudera.mytest \
  -DartifactId=cloudera-mytest \
  -Dversion=10.10.1000 \
  -Dpackage=com.cloudera
cd cloudera-mytest
mvn -Declipse.workspace=$ECLIPSE_WORKSPACE \
  eclipse:configure-workspace eclipse:eclipse clean install
```

## Release

To perform a release:

```bash
# Change the following variables to appropriate values for the target environment
export CF_VERSION_RELEASE=1.5.2
export CDH_VERSION_RELEASE=5.11.0
export CF_VERSION_HEAD=1.5.3
export CDH_VERSION_HEAD=5.11.0
mvn release:prepare -B \
  -DreleaseVersion=$CF_VERSION_RELEASE-cdh$CDH_VERSION_RELEASE \
  -DdevelopmentVersion=$CF_VERSION_HEAD-cdh$CDH_VERSION_HEAD-SNAPSHOT
mvn release:perform -PPKG
git push --all
git tag
```
