#Cloudera Framework

Cloudera framework, providing unit-testing harness and client and runtime bill-of-materials.

##Install

This module can be installed to a local repository:

```bash
mvn clean install
```

Alternatively, the module can be distributed as a binary by embedding the dependencies via the appropriate branch:

```bash
wget -r [https://github.com/ggear/cloudera-framework](https://github.com/ggear/cloudera-framework)
```

##Release

To perform a release:

```bash
export VERSION_RELEASE=0.0.1
export CDH_VERSION_RELEASE=cdh5.4.0
export VERSION_HEAD=0.0.1
export CDH_VERSION_HEAD=cdh5.4.1
mvn release:prepare -B -DreleaseVersion=$VERSION_RELEASE-cdh$CDH_VERSION_RELEASE -DdevelopmentVersion=$VERSION_HEAD-cdh$CDH_VERSION_HEAD-SNAPSHOT
mvn release:clean
```
