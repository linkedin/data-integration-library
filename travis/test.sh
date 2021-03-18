# Copyright 2021 LinkedIn Corporation. All rights reserved.
# Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

#
# Test script used by Travis to test the
# hadoop1 or hadoop2 versions of gobblin.
#

#!/bin/bash
set -e

#free

RUN_TEST_GROUP=${RUN_TEST_GROUP:-default}

script_dir=$(dirname $0)
echo "Old GRADLE_OPTS=$GRADLE_OPTS"

export java_version=$(java -version 2>&1 | grep 'openjdk version' | sed -e 's/openjdk version "\(1\..\).*/\1/')

echo "Using Java version:${java_version}"

export GOBBLIN_GRADLE_OPTS="-Dorg.gradle.daemon=false -Dgobblin.metastore.testing.embeddedMysqlEnabled=false -PusePreinstalledMysql=true -PjdkVersion=${java_version}"

TEST_SCRIPT=${script_dir}/test-${RUN_TEST_GROUP}.sh
if [ -x $TEST_SCRIPT ] ; then
  echo "Running test group $RUN_TEST_GROUP"
  $TEST_SCRIPT "$@"
else
  echo "Test file $TEST_SCRIPT does not exist or is not executable!"
  exit 1
fi
