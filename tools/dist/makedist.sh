#!/bin/bash

# This script is used to create the distribution bundle for an SMV application.
# USAGE: ./tools/makedist.sh smv_dir
# a compressed tar bundle named "bundle.tgz" is created in the application directory.
#
# Note: This script must be executed from the top level directory of an SMV application.
# Note: this script was adopted from a project with scala code (built with maven).

function parse_args()
{
  if [ $# -ne 1 ]; then
    echo "ERROR: missing SMV directory parameter"
    echo "USAGE: $0 _smv_dir_"
    exit 1
  fi

  SMV_HOME="$1"
  if [ ! -d "$SMV_HOME" -o ! -d "${SMV_HOME}/tools" ]; then
    echo "ERROR: invalid SMV home directory parameter"
    exit 1
  fi
}

function build_project()
{
  echo "--- Build project (log = .build.log)"
  mvn clean package >.build.log 2>&1
}

function create_deploy_dir()
{
  echo "--- Create temp deploy dir"
  TMP_DEPLOY_DIR=$(mktemp -d)

  echo "------ copy SMV code"
  (cd "$SMV_HOME";
   find . -name target -prune -o -name .git -prune -o -print0 |
    cpio -p0du "${TMP_DEPLOY_DIR}/SMV")

  echo "------ copy app source"
  find src conf -print0 | cpio -p0du "${TMP_DEPLOY_DIR}"

  echo "------ copy app fat jar"
  mkdir -p "${TMP_DEPLOY_DIR}/target"
  cp target/*jar-with-dependencies.jar "${TMP_DEPLOY_DIR}/target"

  echo "------ copy run script"
  cp tools/run.sh "${TMP_DEPLOY_DIR}"
}

function create_deploy_bundle()
{
  echo "--- Create deploy bundle"
  tar -czf bundle.tgz -C "${TMP_DEPLOY_DIR}" .
}

function cleanup()
{
  if [ ! -z "$TMP_DEPLOY_DIR" ]; then
    echo "--- Removing temp deploy dir"
    rm -rf "$TMP_DEPLOY_DIR"
  fi
}


#--- MAIN ---
set -e
trap cleanup EXIT

parse_args $@
build_project
create_deploy_dir
create_deploy_bundle
