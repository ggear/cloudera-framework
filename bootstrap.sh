#!/usr/bin/env bash

###############################################################################
#
# Bootstrap toolchain based on requirements:
#
# https://github.com/ggear/cloudera-framework#requirements
#
###############################################################################

CF_DIR=$(mktemp -d -t cloudera-framework.XXXXXX)

echo "" && echo "###############################################################################"
CF_VERSION_JAVA=1.8
if [ -z ${JAVA_OPTS+x} ]; then
  export JAVA_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=512m"
fi
if [ $(java -version 2>&1 | grep ${CF_VERSION_JAVA} | wc -l) -eq 0 ]; then
  echo "Unable to install system dependent Java "${CF_VERSION_JAVA}", please do so manually"
fi
echo "###############################################################################"
java -version || { echo "Java "${CF_VERSION_JAVA}" not found" ; return 10; }
echo "###############################################################################"

echo "" && echo "###############################################################################"
CF_VERSION_MAVEN=3.5.2
CF_VERSION_MAVEN_MAJOR=3.
if [ -z ${MAVEN_OPTS+x} ]; then
  export MAVEN_OPTS="-Xmx2g -Dmaven.artifact.threads=15 -XX:ReservedCodeCacheSize=512m -Duser.home=${CF_DIR}"
fi
if [ $(mvn -version 2>&1 | grep ${CF_VERSION_MAVEN_MAJOR} | wc -l) -eq 0 ]; then
  wget http://apache.mirror.amaze.com.au/maven/maven-3/${CF_VERSION_MAVEN}/binaries/apache-maven-${CF_VERSION_MAVEN}-bin.tar.gz -P ${CF_DIR}
  tar xvzf ${CF_DIR}/apache-maven-${CF_VERSION_MAVEN}-bin.tar.gz -C ${CF_DIR}
  test -d ${HOME}/.m2 && cp -rvf ${HOME}/.m2 ${CF_DIR}
  export PATH=${CF_DIR}/apache-maven-${CF_VERSION_MAVEN}/bin:${PATH}
fi
echo "###############################################################################"
mvn -version || { echo "Maven "${CF_VERSION_MAVEN}" not found" ; return 20; }
echo "###############################################################################"

echo "" && echo "###############################################################################"
CF_VERSION_SCALA=2.11.8
CF_VERSION_SCALA_MAJOR=2.11
if [ $(scala -version 2>&1 | grep ${CF_VERSION_SCALA_MAJOR} | wc -l) -eq 0 ]; then
  wget https://downloads.lightbend.com/scala/${CF_VERSION_SCALA}/scala-${CF_VERSION_SCALA}.tgz -P ${CF_DIR}
  tar xvzf ${CF_DIR}/scala-${CF_VERSION_SCALA}.tgz -C ${CF_DIR}
  export PATH=${CF_DIR}/scala-${CF_VERSION_SCALA}/bin:${PATH}
fi
echo "###############################################################################"
scala -version || { echo "Scala "${CF_VERSION_SCALA}" not found" ; return 30; }
echo "###############################################################################"

echo "" && echo "###############################################################################"
CF_VERSION_PYTHON=2.7
if [ $(python --version 2>&1 | grep ${CF_VERSION_PYTHON} | wc -l) -eq 0 ]; then
  echo "Unable to install system dependent CPython "${CF_VERSION_PYTHON}", please do so manually"
fi
echo "###############################################################################"
pip install cm-api altuscli && python --version || { echo "Python "${CF_VERSION_PYTHON}" not found" ; return 40; }
python -m nltk.downloader all
echo "###############################################################################"

echo "" && echo "###############################################################################"

export PATH=$(echo ${PWD}/target/assembly/*/bin):$PATH
