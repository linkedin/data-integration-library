# Copyright 2021 LinkedIn Corporation. All rights reserved.
# Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

#
# Test script used by Travis to test the
# hadoop1 or hadoop2 versions of gobblin.
#

#!/bin/bash
set -e

script_dir=$(dirname $0)

source ${script_dir}/test-groups.inc

echo "Starting $0 at " $(date)
echo "Precompiling tests:"
rm -rf $HOME/.gradle/caches/
./gradlew compileTest -Porg.gradle.parallel=false $GOBBLIN_GRADLE_OPTS
echo "Running tests for $TEST_GROUP1"
time ./gradlew -PskipTestGroup=disabledOnCI -PrunTestGroups=$TEST_GROUP1 -Dorg.gradle.parallel=false $GOBBLIN_GRADLE_OPTS test