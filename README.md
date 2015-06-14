#Cloudera Framework

Provide a Cloudera development framework, including a unit test harness, client and runtime bill-of-materials with full coverage across Cloudera, including HDFS, YARN, MR2, Hive, Spark, HBase, Impala and Search.

##Requirements

To compile, build and package from source, this project requires:

* JDK 1.7
* Maven 3

##Install

This project can be installed to a local repository as per:

```bash
mvn clean install -PCMP
```

Alternatively, the module can be distributed as a binary by embedding (or manually installing) the dependencies via the appropriate branch repository (eg [cloudera-framework-1.0.0-cdh5.4.0](https://github.com/ggear/cloudera-framework/tree/cloudera-framework-1.0.0-cdh5.4.0/cloudera-framework-repo/cloudera-framework-repo-external/src/main/repository) as per [cloudera-cyclehire](https://github.com/ggear/cloudera-cyclehire))

##Usage

Unit test examples are bundled with the module [here](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-main/cloudera-framework-main-test/src/test/java/com/cloudera/framework/main/test). Note that the mini-cluster driven unit tests require significant heap (and perm-gen), the maven surefire plugin is setup with adequate settings but it may be necessary to bump up the default settings in your IDE as necessary.

Unit test runtime logging verbosity can be controlled via log4j, as per the example [log4j.properties](https://raw.githubusercontent.com/ggear/cloudera-framework/master/cloudera-framework-main/cloudera-framework-main-test/src/test/resources/log4j.properties).

The [client](https://raw.githubusercontent.com/ggear/cloudera-framework/master/cloudera-framework-main/cloudera-framework-main-client/pom.xml) and [runtime](https://raw.githubusercontent.com/ggear/cloudera-framework/master/cloudera-framework-main/cloudera-framework-main-runtime/pom.xml) bill-of-materials can be used as POM dependencies.

##Release

To perform a release:

```bash
export VERSION_RELEASE=1.1.0
export CDH_VERSION_RELEASE=5.4.3
export VERSION_HEAD=1.2.0
export CDH_VERSION_HEAD=5.4.3
mvn clean
mvn release:prepare -B -DreleaseVersion=$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE -DdevelopmentVersion=$VERSION_HEAD-cdh$CDH_VERSION_HEAD-SNAPSHOT
mvn release:clean
git checkout -b cloudera-framework-$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE cloudera-framework-$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE
mvn clean install -PCMP
git add -A cloudera-framework-repo/cloudera-framework-repo-external/src/main/repository
git commit -m "Add binaries for cloudera-framework-$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE"
git checkout master
mvn clean
git push --all
git tag
git branch --all
```
