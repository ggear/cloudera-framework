#Cloudera Framework

Cloudera framework, providing a unit-testing harness, client and runtime bill-of-materials.

##Install

This module can be installed to a local repository:

```bash
mvn clean install -PCMP
```

Alternatively, the module can be distributed as a binary by embedding (or manually installing) the dependencies via the appropriate branch repository (eg [cloudera-framework-0.0.4-cdhcdh5.4.0](https://github.com/ggear/cloudera-framework/tree/cloudera-framework-0.0.4-cdhcdh5.4.0/cloudera-framework-repo/cloudera-framework-repo-external/src/main/repository) as per [cloudera-cyclehire](https://github.com/ggear/cloudera-cyclehire))

##Usage

Unit test examples are bundled with the module [here](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-main/cloudera-framework-main-test/src/test/java/com/cloudera/framework/main/test).

The [client](https://raw.githubusercontent.com/ggear/cloudera-framework/master/cloudera-framework-main/cloudera-framework-main-client/pom.xml) and [runtime](https://raw.githubusercontent.com/ggear/cloudera-framework/master/cloudera-framework-main/cloudera-framework-main-runtime/pom.xml) bill-of-materials can be used as POM dependencies.

##Release

To perform a release:

```bash
export VERSION_RELEASE=0.0.4
export CDH_VERSION_RELEASE=cdh5.4.0
export VERSION_HEAD=0.0.5
export CDH_VERSION_HEAD=cdh5.4.0
mvn release:prepare -B -DreleaseVersion=$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE -DdevelopmentVersion=$VERSION_HEAD-cdh$CDH_VERSION_HEAD-SNAPSHOT
mvn release:clean
git checkout -b cloudera-framework-$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE cloudera-framework-$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE
mvn clean install -PCMP
git add -A cloudera-framework-repo/cloudera-framework-repo-external/src/main/repository
git commit -m "Add binaries for cloudera-framework-$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE"
git checkout master
git push --all
git branch --all
```
