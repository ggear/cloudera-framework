# Cloudera Framework

Provide an example organisation wide Cloudera (i.e. Hadoop ecosystem) project framework, 
defining corporate standards on runtime components, datasets, libraries, testing and project 
structure to facilitate operating with a continuous deployment pipeline. This example includes 
[client](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-library/cloudera-framework-library-client)/[runtime](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-library/cloudera-framework-library-runtime)/[thirdparty](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-library/cloudera-framework-library-thirdparty)
bill-of-materials, [utility](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-common/src/main/java/com/cloudera/framework/common)/[driver](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-common/src/main/java/com/cloudera/framework/common/Driver.java)
libraries and a unit test [harness](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-testing/src/main/java/com/cloudera/framework/testing)
with [examples](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-testing/src/test/java/com/cloudera/framework/testing/server),
providing full coverage against CDH:

* MR2
* Kudu
* HDFS
* Flume
* Kafka
* Impala
* ZooKeeper
* Spark & Spark2
* Hive/MR & Hive/Spark

[Examples](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example) 
are included, codifying the standards, providing end to end data streaming, ingest, modeling, testing
pipelines, with synthetic datasets to exercise the codebase.

Finally, a set of [archetypes](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype) 
are included to provide bare bones starter client modules.

# Requirements

To compile, build and package from source, this project requires:

* Java 8
* Maven 3
* Scala 2.11
* Python 2.7
* Anaconda 4

The [bootstrap.sh](https://github.com/ggear/cloudera-framework/blob/master/bootstrap.sh) script tests for,
configures and installs (where possible) the required toolchain and should be sourced as so:

```bash
source bootstrap.sh
```

To run the unit and integrations tests, binaries and meta-data are provided for all CDH components:

* MacOS 10.12.x
* CentOS/RHEL 6.x
* CentOS/RHEL 7.x
* Ubuntu LTS 14.04.x

Some CDH components have additional coverage on other platforms and versions.

Note that in addition to Maven dependencies, Cloudera 
[parcels](http://www.cloudera.com/documentation/enterprise/latest/topics/cm_ig_parcels.html) 
are used to manage platform dependent binaries by way of the 
[cloudera-parcel-plugin](https://github.com/ggear/cloudera-parcel/tree/master/cloudera-parcel-plugin).

# Limitations

As above, this code is known to not work out of the box on Windows hosts, only Linux and MacOS
are supported. If developing on Windows it is recommended to run a Linux VM and develop from within it.  

In addition, it is known that running the code from remote storage (eg NFS) will not only perform poorly,
but will likely result in timing errors during testing, so it is recommended that only direct attached
storage (be it physical or para-virtualized) by used.

# Install

This project can be installed to a local repository, skipping tests, as per:

```bash
git clone git@github.com:ggear/cloudera-framework.git
cd cloudera-framework
mvn install -PPKG
```

To run the tests:

```bash
mvn test
mvn test -pl cloudera-framework-testing -PSCALA_2.11
```

Note that localhost must be resolvable to run the tests.

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
      <version>1.5.5-cdh5.12.0</version>
      <type>pom</type>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>com.cloudera.framework</groupId>
      <artifactId>cloudera-framework-testing</artifactId>
      <version>1.5.5-cdh5.12.0</version>
      <scope>test</scope>
    </dependency>
  </dependencies>
</project>
```

# Usage

The cloudera-framework includes a set of 
[examples](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example) 
which exercise the framework:

* [Example 1](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example/cloudera-framework-example-1) 
  (Java, HSQL, Flume, MR, Hive/MR, Impala, HDFS)
* [Example 2](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example/cloudera-framework-example-2)
  (Java, HSQL, Kafka, Hive/Spark, Spark, Impala, S3)
* [Example 3](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example/cloudera-framework-example-3) 
  (Scala, CDSW, Spark2, MLlib, PMML, HDFS)
* [Example 4](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example/cloudera-framework-example-4) 
  (Java, Kafka, Spark2 Streaming, Kudu, HDFS)
* [Example 5](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example/cloudera-framework-example-5) 
  (Python, NLTK, PySpark, Spark2, HDFS)

In addition,
[archetypes](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype)
are available in various profiles, allowing one to stand up a
bare bones project to bootstrap new cloudera-framework client projects:

* [Workload](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype/cloudera-framework-archetype-workload/src/main/resources/archetype-resources)
  (Scala, Java, Spark2)

For example, a project could be created with the workload profile baseline,
including very simple example via:

```bash
# Change the following variables to appropriate values for the target environment
export CF_VERSION=1.5.5
export CDH_VERSION=5.12.0
export CF_PROFILE=workload
mvn archetype:generate -B \
  -DarchetypeRepository=http://52.63.86.162/artifactory/cloudera-framework-releases \
  -DarchetypeGroupId=com.cloudera.framework.archetype \
  -DarchetypeArtifactId=cloudera-framework-archetype-$CF_PROFILE \
  -DarchetypeVersion=$CF_VERSION-cdh$CDH_VERSION \
  -DgroupId=com.cloudera.mytest \
  -DartifactId=cloudera-mytest \
  -Dpackage=com.cloudera.mytest
```

# Release

To perform a release:

```bash
# Change the following variables to appropriate values for the target release
export CF_VERSION_RELEASE=1.5.5
export CDH_VERSION_RELEASE=5.12.0
export CF_VERSION_HEAD=1.5.6
export CDH_VERSION_HEAD=5.12.0
mvn clean install && \
mvn test -pl cloudera-framework-testing -PSCALA_2.11 && \
mvn release:prepare -B \
  -DreleaseVersion=$CF_VERSION_RELEASE-cdh$CDH_VERSION_RELEASE \
  -DdevelopmentVersion=$CF_VERSION_HEAD-cdh$CDH_VERSION_HEAD-SNAPSHOT && \
mvn release:perform -PPKG && \
git push --all && \
git tag
```
