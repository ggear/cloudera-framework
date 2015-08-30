#Cloudera Framework

Provide an example organisation core Cloudera Framework, defining corporate standards on runtime components, dependencies, datasets, utility libraries, unit testing and project structure. The example includes a unit test harness, clients/runtime bill-of-materials and driver base class, with full coverage across the Cloudera stack, including:

* HDFS
* YARN
* MR2
* Hive
* Flume
* Kafka

A Maven archetype is also included, codifying the standards and setting expectations.

##Requirements

To compile, build and package from source, this project requires:

* JDK 1.7
* Maven 3

##Install

This project can be installed to a local repository as per:

```bash
export CF_VERSION=1.2.0
export CDH_VERSION=5.4.4
git clone git@github.com:ggear/cloudera-framework.git
cd cloudera-framework
git branch --all
git checkout cloudera-framework-$CF_VERSION-cdh$CDH_VERSION
mvn clean install -PCMP
```

Alternatively, the module can be distributed as a binary by copying the dependencies (eg [cloudera-framework-1.2.0-cdh5.4.4](https://github.com/ggear/cloudera-framework/tree/cloudera-framework-1.2.0-cdh5.4.4/cloudera-framework-repo/cloudera-framework-repo-external/src/main/repository) into a shared lib (eg, [cloudera-cyclehire](https://github.com/ggear/cloudera-cyclehire)).

##Usage

The cloudera-framework includes an archetype allowing you to stand up a simple, but best practices project that shows how to use the framework and get going fast:

```bash
export CF_VERSION=1.2.0
export CDH_VERSION=5.4.4
export ECLIPSE_WORKSPACE=/tmp/eclipse/workspace
mvn archetype:generate \
  -DarchetypeGroupId=com.cloudera.framework.main \
  -DarchetypeArtifactId=cloudera-framework-main-archetype \
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
export CF_VERSION_RELEASE=1.2.0
export CDH_VERSION_RELEASE=5.4.4
export CF_VERSION_HEAD=1.2.0
export CDH_VERSION_HEAD=5.4.5
mvn clean
mvn release:prepare -B -DreleaseVersion="$CF_VERSION_RELEASE"-cdh"$CDH_VERSION_RELEASE" -DdevelopmentVersion="$CF_VERSION_HEAD"-cdh"$CDH_VERSION_HEAD-SNAPSHOT" -PCMP
mvn release:clean
git checkout -b cloudera-framework-$CF_VERSION_RELEASE-cdh$CDH_VERSION_RELEASE cloudera-framework-$CF_VERSION_RELEASE-cdh$CDH_VERSION_RELEASE
mvn clean install -PCMP
git add -A cloudera-framework-repo/cloudera-framework-repo-external/src/main/repository
git commit -m "Add binaries for cloudera-framework-$CF_VERSION_RELEASE-cdh$CDH_VERSION_RELEASE"
git checkout master
mvn clean
git push --all
git tag
git branch --all
```
