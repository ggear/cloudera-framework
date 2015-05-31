#Cloudera Framework

Cloudera framework, providing a unit-testing harness, client and runtime bill-of-materials.

##Install

This module can be installed to a local repository:

```bash
mvn clean install
```

Alternatively, the module can be distributed as a binary by embedding (or manually installing) the dependencies via the appropriate branch repository (eg [cloudera-framework-0.0.1-cdhcdh5.4.0](https://github.com/ggear/cloudera-framework/tree/cloudera-framework-0.0.1-cdhcdh5.4.0/cloudera-framework-repo/cloudera-framework-repo-external/src/main/repository))

##Release

To perform a release:

```bash
export VERSION_RELEASE=0.0.1
export CDH_VERSION_RELEASE=cdh5.4.0
export VERSION_HEAD=0.0.1
export CDH_VERSION_HEAD=cdh5.4.1
mvn release:prepare -B -DreleaseVersion=$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE -DdevelopmentVersion=$VERSION_HEAD-cdh$CDH_VERSION_HEAD-SNAPSHOT
mvn release:clean
git checkout -b cloudera-framework-$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE cloudera-framework-$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE
mvn clean install -Dmaven.test.skip=true
git add -A cloudera-framework-repo/cloudera-framework-repo-external/src/main/repository
git commit -m "Add binaries for cloudera-framework-$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE"
git checkout master
git push --all
git branch --all
```
