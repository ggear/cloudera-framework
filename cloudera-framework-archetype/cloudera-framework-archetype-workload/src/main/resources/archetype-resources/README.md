# ${artifactId}

This project was created from the baseline shipped as part of a
[cloudera-framework-archetype](https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-archetype)
and includes the following:

* Scala Spark2 driver
* Java test harness
* Synthetic test data
* Parcel packaging
* Altus bootstrap script

# Requirements

This project shares requirements with those specified in the
[cloudera-framework](https://github.com/ggear/cloudera-framework#requirements).

# Install

To compile, test and package the project, the following should be run
in the root directory:

```bash
mvn clean install
```

There is also a convienience compile only profile:

```bash
mvn clean install -PCMP
```

and a package only profile:

```bash
mvn clean install -PPKG
```

Tests can be turned on/off via the standard surefire switches:

```bash
mvn clean install -DskipTests
```
