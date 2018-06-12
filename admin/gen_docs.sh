#!/bin/bash


if [ "$#" -ne 1 ]; then
  echo "ERROR: Invalid number of arguments"
  echo "USAGE: $0 output_path"

  echo "example:"
  echo "  \$ $0 /tmp/docgen"
  exit 1
fi

OUTPUT_DIR=$1


function define_vars()
{
  SMV_TOOLS="$(cd "`dirname "$0"`"; pwd)"
  PKG_TO_DOC="$SMV_TOOLS/../src/main/python/smv"
  PKG_DIR=$(dirname $PKG_TO_DOC)
  PYDOC_DIR="$OUTPUT_DIR/pythondocs"
  SCALADOC_DIR="$OUTPUT_DIR/scaladocs"

  # This will be used by sphinx-conf.py
  export SMV_VERSION=$(cat "$SMV_TOOLS/../.smv_version")

  if [ -z $SPARK_HOME ]; then
    # might be able to use tools/_env.sh to set SPARK_HOME
    SPARK_HOME="$(dirname $(which spark-submit))/.."
  fi

  [ -z $SPARK_HOME ] && echo "ERROR: can't find spark" && exit 1

  export PYTHONPATH="$PKG_DIR:$PYTHONPATH"
  # Need pyspark and py4j the sys.path so they can be imported by sphinx
  export PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
  export PYTHONPATH="$(ls -1 $SPARK_HOME/python/lib/py4j-*-src.zip):$PYTHONPATH"
}

function  build_pydocs()
{
  # build the python docs
  echo "-- building pythondocs..."
  rm -rf $PYDOC_DIR
  mkdir -p $PYDOC_DIR
  sphinx-apidoc --full -o $PYDOC_DIR $PKG_TO_DOC
  cp $SMV_TOOLS/conf/sphinx-conf.py $PYDOC_DIR/conf.py
  (cd $PYDOC_DIR; make html)
}

function build_scaladocs()
{
  # build the scala docs
  echo "-- building scaladocs..."
  sbt doc
  mkdir -p $SCALADOC_DIR
  cp -R ${SMV_TOOLS}/../target/scala-*/api/* $SCALADOC_DIR
}



define_vars
build_pydocs
build_scaladocs
