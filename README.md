# Cloudera Framework

Provide an example organisation wide Cloudera (i.e. Hadoop ecosystem) project framework, 
defining corporate standards on runtime components, datasets, libraries, testing, deployment and
project  structure to facilitate operating within a continuous deployment pipeline. This example includes 
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

The framework can target managed services provisioned by [Cloudera Altus](https://www.cloudera.com/products/altus.html),
automated cluster deployments via [Cloudera Director](https://www.cloudera.com/products/product-components/cloudera-director.html)
and or manually managed clusters via [Cloudera Manager](https://www.cloudera.com/products/product-components/cloudera-manager.html).

[Examples](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example) 
are included, codifying the standards, providing end to end data streaming, ingest, modeling, testing
pipelines, with synthetic datasets to exercise the codebase.

Finally, a set of [archetypes](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype) 
are included to provide bare bones starter client modules.

## Requirements

To compile, build and package from source, this project requires:

* Java 8
* Maven 3
* Scala 2.11
* Python 2.7
* Anaconda 4
* Cloudera Altus CLI 2.2
* Cloudera Director Client 2.6
* Python Cloudera Manager API 5

The [bootstrap.sh](https://github.com/ggear/cloudera-framework/blob/master/bootstrap.sh) script tests for,
configures and installs (where possible) the required toolchain and should be sourced as so:

```bash
. ./bootstrap.sh environment
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

## Limitations

As above, this code is known to not work out of the box on Windows hosts, only Linux and MacOS
are supported. If developing on Windows it is recommended to run a Linux VM and develop from within it.  

## Install

This project can be compiled, packaged and installed to a local repository, skipping tests, as per:

```bash
mvn install -PPKG
```

To only compile the project:

```bash
mvn install -PCMP
```

To run the tests for both Scala 2.10 (default) and 2.11 (localhost must be resolvable to run the tests):

```bash
mvn test
mvn test -pl cloudera-framework-testing -PSCALA_2.11
```

The bootstrap script provides convenience mechanisms to build and release the project as so:

```bash
./bootstrap.sh build release
```

As an alternative to installing to a local repository, the module can be included as a binary dependency in maven, 
for example the core client bill-of-materials and test harness can be pulled into a module as below:

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
      <version>1.9.4-cdh5.12.2</version>
      <type>pom</type>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>com.cloudera.framework</groupId>
      <artifactId>cloudera-framework-testing</artifactId>
      <version>1.9.4-cdh5.12.2</version>
      <scope>test</scope>
    </dependency>
  </dependencies>
</project>
```

## Usage

The cloudera-framework includes a set of 
[examples](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example):

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
are available in various profiles, allowing one to bootstrap a new cloudera-framework client module:

* [Workload](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype/cloudera-framework-archetype-workload/src/main/resources/archetype-resources)
  (Scala, Java, Spark2)

For example, a project could be created with the workload profile baseline,
including a very simple example targeting a Cloudera Altus runtime as below:

```bash
mvn org.apache.maven.plugins:maven-archetype-plugin:2.4:generate -B \
  -DarchetypeRepository=http://52.63.86.162/artifactory/cloudera-framework-releases \
  -DarchetypeGroupId=com.cloudera.framework.archetype \
  -DarchetypeArtifactId=cloudera-framework-archetype-workload \
  -DarchetypeVersion=1.9.4-cdh5.12.2 \
  -DgroupId=com.myorg.mytest \
  -DartifactId=mytest \
  -Dpackage=com.myorg.mytest \
  -DaltusEnv=my_altus_environment \
  -DaltusCluster=my_cluster \
  -DaltusS3Bucket=my_s3_bucket
```

Note that in order to run against the Cloudera Altus Amazon AWS runtime as above, 
both the "AWS_ACCESS_KEY" and "AWS_SECRET_KEY" are required to be set in the 
environment and each Maven archetype parameter with the "altus" prefix has to be 
given an appropriate value. The "altusS3Bucket" parameter should specify a valid
S3 bucket which the user has read/write access to, within the "altusEnv" region
and has data stored under the "/data/workload/input" key with schema like the
[pristine](https://raw.githubusercontent.com/ggear/cloudera-framework/master/cloudera-framework-archetype/cloudera-framework-archetype-workload/src/main/resources/archetype-resources/src/test/resources/data/__artifactId__/csv/pristine/pristine.csv) 
test data set.

