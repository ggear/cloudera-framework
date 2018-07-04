#!/usr/bin/env bash

###############################################################################
#
# Bootstrap toolchain based on requirements:
#
# https://github.com/ggear/cloudera-framework#requirements
#
###############################################################################

[[ -z "${CDSW_PROJECT_URL}" ]] && [[ -f "/etc/profile" ]] && . /etc/profile

CF_DIR=$(mktemp -d -t cloudera-framework.XXXXXX)

function mode_execute {

  if [ "${MODE}" = "environment" ]; then

    echo "" && echo "" && echo "" && echo "Environment [cloudera-framework]"
    CF_VERSION_JAVA=1.8
    if [ -z ${JAVA_OPTS+x} ]; then
      export JAVA_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=512m"
    fi
    if [ $(java -version 2>&1 | grep ${CF_VERSION_JAVA} | wc -l) -eq 0 ]; then
      echo "Unable to install system dependent Java "${CF_VERSION_JAVA}", please do so manually"
    fi
    java -version || { echo "Java "${CF_VERSION_JAVA}" not found" ; return 10; }
    CF_VERSION_MAVEN=3.5.3
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
    mvn -version || { echo "Maven "${CF_VERSION_MAVEN}" not found" ; return 20; }
    CF_VERSION_SCALA=2.11.8
    CF_VERSION_SCALA_MAJOR=2.11
    if [ $(scala -version 2>&1 | grep ${CF_VERSION_SCALA_MAJOR} | wc -l) -eq 0 ]; then
      wget https://downloads.lightbend.com/scala/${CF_VERSION_SCALA}/scala-${CF_VERSION_SCALA}.tgz -P ${CF_DIR}
      tar xvzf ${CF_DIR}/scala-${CF_VERSION_SCALA}.tgz -C ${CF_DIR}
      export PATH=${CF_DIR}/scala-${CF_VERSION_SCALA}/bin:${PATH}
    fi
    scala -version || { echo "Scala "${CF_VERSION_SCALA}" not found" ; return 30; }
    CF_VERSION_PYTHON=2.7
    if [ $(python --version 2>&1 | grep ${CF_VERSION_PYTHON} | wc -l) -eq 0 ]; then
      echo "Unable to install system dependent CPython "${CF_VERSION_PYTHON}", please do so manually"
    fi
    pip install cm-api altuscli && python --version || { echo "Python "${CF_VERSION_PYTHON}" not found" ; return 40; }
    if [ -w "/usr/local/share/nltk_data" ]; then
      python -m nltk.downloader -d /usr/local/share/nltk_data all
    else
      python -m nltk.downloader all
    fi
    CF_VERSION_CONDA=4.5
    if [ $(conda --version 2>&1 | grep ${CF_VERSION_CONDA} | wc -l) -eq 0 ]; then
      echo "Unable to install system dependent Conda "${CF_VERSION_PYTHON}", please do so manually"
    fi
    conda --version || { echo "Conda "${CF_VERSION_CONDA}" not found" ; return 50; }
    conda config --set ssl_verify no
    export PATH=$(echo ${PWD}/target/assembly/*/bin):$PATH

  elif [ "${MODE}" = "versions" ]; then

    echo "" && echo "" && echo "" && echo "Versions [cloudera-framework]"
    VERSION_OLD=$(grep -m 1 "<version>" pom.xml | sed 's/<version>//' | sed 's/<\/version>//' | sed 's/-SNAPSHOT*//' | xargs)
    echo "Type the CDH version you want to baseline on followed by [ENTER]:" && read VERSION_CDH
    VERSION_NEW=$(git describe \-\-tags | cut -c20-34 | cut -f1 -d"-" | xargs)"-cdh"$VERSION_CDH"-SNAPSHOT"
    git pull --all
    mvn install -PCMP
    mvn versions:set -DnewVersion=$VERSION_NEW
    mvn versions:commit
    mvn clean install -PPKG
    mvn clean
    [ $(grep -Fr $VERSION_OLD * | grep -v README.md | grep -v dependency-reduced-pom.xml | tee /dev/tty | wc -l) -ne 0 ] && echo "Error, references to old version [$VERSION_OLD] detected" && exit 1
    git diff
    git status


  elif [ "${MODE}" = "build" ]; then

    echo "" && echo "" && echo "" && echo "Build [cloudera-framework]"
    git checkout master
    git pull -a
    mvn install -PPKG
    mvn clean install
    mvn test -pl cloudera-framework-testing -PSCALA_2.11

  elif [ "${MODE}" = "release" ]; then

    echo "" && echo "" && echo "" && echo "Release [cloudera-framework]"
    [[ -n "$(git status --porcelain)" ]] && exit 1
    git checkout master
    git remote set-url origin git@github.com:ggear/cloudera-framework.git
    mvn install -PCMP
    VERSION_PREVIOUS=$(git describe \-\-tags | cut -c20-34 | xargs)
    VERSION_RELEASE=$(grep -m 1 "<version>" pom.xml | sed 's/<version>//' | sed 's/<\/version>//' | sed 's/-SNAPSHOT*//' | xargs)
    VERSION_HEAD_NUMERIC=$(($(echo $VERSION_RELEASE | cut -f1 -d"-" | sed 's/\.//g')+1))
    VERSION_HEAD=${VERSION_HEAD_NUMERIC:0:1}.${VERSION_HEAD_NUMERIC:1:1}.${VERSION_HEAD_NUMERIC:2:2}"-"$(grep -m 1 "<version>" pom.xml | sed 's/<version>//' | sed 's/<\/version>//' | sed 's/-SNAPSHOT*//' |  cut -d"-" -f2)"-SNAPSHOT"
    mvn clean install
    mvn test -pl cloudera-framework-testing -PSCALA_2.11
    mvn release:prepare -B -DreleaseVersion=$VERSION_RELEASE -DdevelopmentVersion=$VERSION_HEAD -PPKG
    mvn release:perform -PPKG
    sed -i -e "s/$VERSION_PREVIOUS/$VERSION_RELEASE/g" README.md
    git add -A
    git commit -m "Update README for cloudera-framework-${VERSION_RELEASE}"
    git push --all
    git tag

  else

    echo "Usage: ${0} <environment|versions|build|release>"

  fi

}

TIME=$(date +%s)

for MODE in "$@"; do
  [[ ! "${MODE}" = "environment" ]] && set -x -e
  mode_execute
done

set +x
TIME="$(($(date +%s) - $TIME))"
echo "" && echo "Pipeline execution took ["$(printf '%02d:%02d:%02d\n' $(($TIME/3600)) $(($TIME%3600/60)) $(($TIME%60)))"] time" && echo ""
