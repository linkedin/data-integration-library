# Copyright 2021 LinkedIn Corporation. All rights reserved.
# Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

#!/bin/bash
set -e

echo "Starting $0 at " $(date)
time ./gradlew clean build -x test -x javadoc -Dorg.gradle.parallel=true $DIL_GRADLE_OPTS
