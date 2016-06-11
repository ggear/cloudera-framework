#Cloudera Framework

Provide an example organisation wide Cloudera (i.e. Hadoop ecosystem) project framework, defining corporate standards on runtime components, datasets, libraries, testing and project structure to facilitate operating with a continuous deployment model. This example includes [client](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-client) and [runtime](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-runtime) bill-of-materials, [utlitiy](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-common/src/main/java/com/cloudera/framework/common) libraries, [driver](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-common/src/main/java/com/cloudera/framework/common/Driver.java) and a unit test [harness](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-testing/src/main/java/com/cloudera/framework/testing) with [examples](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-testing/src/test/java/com/cloudera/framework/testing/server), providing full coverage against CDH, including:

* HDFS
* YARN
* Kudu
* Impala
* Spark
* MR2
* Hive
* Flume
* Kafka

A Maven [archetype](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype) is also included, codifying the standards, providing an example end to end data [streaming](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype/src/main/resources/archetype-resources/src/main/java/com/cloudera/example/stream), [ingest](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype/src/main/resources/archetype-resources/src/main/java/com/cloudera/example/ingest), [modeling](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype/src/main/resources/archetype-resources/src/main/java/com/cloudera/example/model), [testing](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype/src/main/resources/archetype-resources/src/test/java/com/cloudera/example) and synthetic [datasets](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype/src/main/resources/archetype-resources/src/test/resources/data/mydataset) to exercise the codebase.

##Requirements

To compile, build and package from source, this project requires:

* JDK 1.7
* Maven 3

To run the unit and integrations tests, binaries and meta-data are provided for all CDH componnents:

* OS-X 10.11.x
* CentOS/RHEL 6.x
* CentOS/RHEL 7.x
* Ubuntu LTS 14.04.x

Some CDH components have additional coverage on other platforms and versions.

Note that in addition to Maven dependencies, Cloudera [parcels](http://www.cloudera.com/documentation/enterprise/latest/topics/cm_ig_parcels.html) are used to manage platform dependent binaries. This project depends on the [cloudera-parcel-plugin](https://github.com/ggear/cloudera-parcel/cloudera-parcel-plugin) to help manage its parcel dependencies.

##Install

This project can be installed to a local repository as per:

```bash
# Change the following variables to appropriate values for your target environment
export CF_VERSION=1.4.2
export CDH_VERSION=5.7.0
git clone git@github.com:ggear/cloudera-framework.git
cd cloudera-framework
git branch -all
git checkout cloudera-framework-$CF_VERSION-cdh$CDH_VERSION
mvn install -PPKG
```

Alternatively, the module can be included as a binary dependency in maven, an example of pulling in the core client bill-of-matierals and test harness is seen below:

```xml
	<repositories>
		<repository>
			<id>cloudera-framework</id>
			<url>http://52.63.86.162:80/artifactory/libs-release-local</url>
			<snapshots>
				<enabled>false</enabled>
				<updatePolicy>never</updatePolicy>
			</snapshots>
		</repository>
	</repositories>
	<dependencies>
		<dependency>
			<groupId>com.cloudera.framework.client</groupId>
			<artifactId>cloudera-framework-client-core</artifactId>
			<version>1.4.2-cdh5.7.0</version>
			<type>pom</type>
			<scope>provided</scope>
		</dependency>
		<dependency>
			<groupId>com.cloudera.framework</groupId>
			<artifactId>cloudera-framework-testing</artifactId>
			<version>1.4.2-cdh5.7.0</version>
			<scope>test</scope>
		</dependency>
	</dependencies>
```

##Usage

The cloudera-framework includes an [example](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-example) module which exercises the entire codebase. In addition, an archetype is available allowing you to stand up a simple, bare bones project for new cloudera-framework clients as so:

```bash
# Change the following variables to appropriate values for your target environment
export CF_VERSION=1.4.2
export CDH_VERSION=5.7.0
export ECLIPSE_WORKSPACE=/tmp/eclipse/workspace
mvn archetype:generate \
  -DarchetypeRepository=http://52.63.86.162/artifactory/libs-release-local \
  -DarchetypeGroupId=com.cloudera.framework \
  -DarchetypeArtifactId=cloudera-framework-archetype \
  -DarchetypeVersion=$CF_VERSION-cdh$CDH_VERSION \
  -Dversion=1.0.0-SNAPSHOT \
  -DgroupId=com.my.company \
  -DartifactId=my-cloudera-project \
  -Dpackage="com.my.company"
cd my-cloudera-project
mvn -Declipse.workspace=$ECLIPSE_WORKSPACE eclipse:configure-workspace eclipse:eclipse clean install
```

##Release

To perform a release:

```bash
# Change the following variables to appropriate values for your target environment
export CF_VERSION_RELEASE=1.4.2
export CDH_VERSION_RELEASE=5.7.0
export CF_VERSION_HEAD=1.4.3
export CDH_VERSION_HEAD=5.7.0
rm -rf ~/.m2/repository/com/cloudera/parcel/cloudera-*
mvn release:prepare -B -DreleaseVersion=$CF_VERSION_RELEASE-cdh$CDH_VERSION_RELEASE -DdevelopmentVersion=$CF_VERSION_HEAD-cdh$CDH_VERSION_HEAD-SNAPSHOT
mvn release:perform
git push --all
git tag
```
