#Cloudera Framework

Provide an example organisation core Cloudera Framework, defining corporate standards on runtime components, datasets, libraries, testing and project structure. This example includes [client](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-client) and [runtime](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-runtime) bill-of-materials, [utlitiy libraries](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-common/src/main/java/com/cloudera/framework/common), [driver base class](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-common/src/main/java/com/cloudera/framework/common/Driver.java), a [unit test harness](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-testing/src/test/java/com/cloudera/framework/test) and a parcel management [maven plugin](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-plugin) with full coverage against CDH, including:

* HDFS
* YARN
* Kudu
* Impala
* Spark
* MR2
* Hive
* Flume
* Kafka

A Maven archetype is also included, codifying the standards, providing an example end to end data ingest and processing pipeline, including synthetic datasets to exercise the codebase.

##Requirements

To compile, build and package from source, this project requires:

* JDK 1.7
* Maven 3

##Install

This project can be installed to a local repository as per:

```bash
# Change the following variables to appropriate values for your target environment
export CF_VERSION=1.3.0
export CDH_VERSION=5.5.1
git clone git@github.com:ggear/cloudera-framework.git
cd cloudera-framework
git branch -all
git checkout cloudera-framework-$CF_VERSION-cdh$CDH_VERSION
mvn install -PSTG1
mvn install
```

Alternatively, the module can be distributed as a binary by copying the dependencies (eg [cloudera-framework-1.3.0-cdh5.5.1](https://github.com/ggear/cloudera-framework/tree/cloudera-framework-1.3.0-cdh5.5.1/cloudera-framework-thirdparty/src/main/repository) into a shared lib (eg, [cloudera-cyclehire](https://github.com/ggear/cloudera-cyclehire)).

##Usage

The cloudera-framework includes an archetype allowing you to stand up a simple, best practices project that shows how to use the framework and get going fast:

```bash
# Change the following variables to appropriate values for your target environment
export CF_VERSION=1.3.0
export CDH_VERSION=5.5.1
export ECLIPSE_WORKSPACE=/tmp/eclipse/workspace
mvn archetype:generate \
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
export CF_VERSION_RELEASE=1.3.0
export CDH_VERSION_RELEASE=5.5.1
export CF_VERSION_HEAD=1.3.1
export CDH_VERSION_HEAD=5.5.1
mvn clean install -PSTG1,CMP
mvn clean install -PSTG2,CMP
mvn release:prepare -B -DreleaseVersion=$CF_VERSION_RELEASE-cdh$CDH_VERSION_RELEASE -DdevelopmentVersion=$CF_VERSION_HEAD-cdh$CDH_VERSION_HEAD-SNAPSHOT
mvn release:clean
git checkout -b cloudera-framework-$CF_VERSION_RELEASE-cdh$CDH_VERSION_RELEASE cloudera-framework-$CF_VERSION_RELEASE-cdh$CDH_VERSION_RELEASE
git status
git add -A cloudera-framework-thirdparty/src/main/repository
git status
git commit -m "Add binaries for cloudera-framework-$CF_VERSION_RELEASE-cdh$CDH_VERSION_RELEASE"
git checkout master
mvn clean
git push --all
git tag
git branch --all
```
