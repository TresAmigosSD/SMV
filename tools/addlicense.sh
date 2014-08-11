#!/bin/bash
#
# script to add the apache license header to all the scala files in the
# project.  It will only add the license header if it is not there already.
#
# USAGE: (from top level SMV directory)
# $ ./tools/addlicense.sh
#

LICENSE_FILE="./tools/license_header.txt"

find src -name '*.scala' -print | while read f; do
  c=$(head -10 $f | grep -c "LICENSE-2.0")
  if [[ "$c" == 0 ]]; then
    echo "Adding license header to: $f"
    mv $f $f.bak
    cp ${LICENSE_FILE} $f
    cat $f.bak >> $f
    rm -f $f.bak
  fi
done
