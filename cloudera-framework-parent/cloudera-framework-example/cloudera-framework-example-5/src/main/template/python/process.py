###############################################################################
# ${TEMPLATE.PRE-PROCESSOR.LINE}${TEMPLATE.PRE-PROCESSOR.RAW_TEMPLATE}
#
# This file is in the ${TEMPLATE.PRE-PROCESSOR.STATE} pre-processed state with template available by the
# same package and file name under the modules src/main/template directory.
#
# When editing the template directly (as indicated by the presence of the
# TEMPLATE.PRE-PROCESSOR.RAW_TEMPLATE tag at the top of this file), care should
# be taken to ensure the maven-resources-plugin generate-sources filtering of the
# TEMPLATE.PRE-PROCESSOR tags, which comment and or uncomment blocks of the
# template, leave the file in a consistent state, as a script ot library,
# post filtering.
#
# It is desirable that in template form, the file remains both compilable and
# runnable as a script in your IDEs (eg Eclipse, IntelliJ, CDSW etc). To setup
# your environment, it may be necessary to run the pre-processed script once
# (eg to execute AddJar commands with dependency versions completely resolved) but
# from then on the template can be used for direct editing and distribution via
# the source code control system and maven repository for dependencies.
#
# The library can be tested during the standard maven compile and test phases.
#
# Note that pre-processed files will be overwritten as part of the Maven build
# process. Care should be taken to either ignore and not edit these files (eg
# libraries) or check them in and note changes post Maven build (eg scripts)
#
# This file was adapted from a project authored by Michiaki Ariga:
# https://github.com/chezou/NLTK-pyspark
#
###############################################################################

from pyspark.sql import SparkSession

# @formatter:off
#${TEMPLATE.PRE-PROCESSOR.LINE}!hdfs dfs -rm -f -r -skipTrash /tmp/stateunion
#${TEMPLATE.PRE-PROCESSOR.LINE}!hdfs dfs -mkdir -p /tmp/stateunion/landing
#${TEMPLATE.PRE-PROCESSOR.LINE}!hdfs dfs -put cloudera-framework-parent/cloudera-framework-example-5/src/test/resources/data/stateunion/nixon/1970/1970-Nixon.txt /tmp/stateunion/landing/1970-Nixon.txt
#${TEMPLATE.PRE-PROCESSOR.LINE}!hdfs dfs -ls /tmp/stateunion/landing
# @formatter:on

sparkSession = SparkSession.builder \
    .appName("cloudera-framework-example-5") \
    .getOrCreate()

data = sparkSession.sparkContext.textFile(
    '/tmp/stateunion/landing/1970-Nixon.txt')


def word_tokenize(x):
    import nltk
    return nltk.word_tokenize(x)


def word_position(x):
    import nltk
    return nltk.pos_tag([x])


words = data.flatMap(word_tokenize)
words.collect()
words.saveAsTextFile(
    '/tmp/stateunion/processed/words')

words_positions = words.map(word_position)
words_positions.collect()
words_positions.saveAsTextFile(
    '/tmp/stateunion/processed/words_positions')
