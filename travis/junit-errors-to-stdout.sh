# Copyright 2021 LinkedIn Corporation. All rights reserved.
# Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

#
# Script used by Travis builds upon a failure to format and
# print the failing test results to the console.
#

#!/bin/bash
IFS='
'
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOTDIR="$1"
if [ -z "$ROOTDIR" ]; then
	ROOTDIR="."
fi
echo 'Formatting results...'
FILES=$(find "$ROOTDIR" -path '*/build/*/test-results/*.xml' | python "$DIR/filter-to-failing-test-results.py")
if [ -n "$FILES" ]; then
	for file in $FILES; do
		echo "Formatting $file"
		if [ -f "$file" ]; then
			echo '====================================================='
			xsltproc "$DIR/junit-xml-format-errors.xsl" "$file"
		fi
	done
	echo '====================================================='
else
	echo 'No */build/*/test-results/*.xml files found with failing tests.'
fi
