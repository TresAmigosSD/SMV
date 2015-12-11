#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script builds the SMV SparkR package.  It is based on the install-dev.sh script from sparkR.

# skip install if we cant find the R command.
R_CMD_PATH=$(type -p R)
if [ -z "${R_CMD_PATH}" ]; then
  echo "Skipping install of R packages due to missing R command"
  exit 0
fi

set -e

FWDIR="$(cd `dirname $0`; pwd)"
LIB_DIR="$FWDIR/lib"

mkdir -p $LIB_DIR

pushd $FWDIR > /dev/null

# Generate Rd files if devtools is installed
Rscript -e ' if("devtools" %in% rownames(installed.packages())) { library(devtools); devtools::document(pkg="./smvR", roclets=c("rd")) }'

# Install SparkR to $LIB_DIR
R CMD INSTALL --library=$LIB_DIR $FWDIR/smvR/

popd > /dev/null

