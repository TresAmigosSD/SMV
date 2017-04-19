#!/bin/bash

SMV_TOOLS="$(cd "`dirname "$0"`"; pwd)"
PKG_TO_DOC="$SMV_TOOLS/../src/main/python/smv"
PKG_DIR=$(dirname $PKG_TO_DOC)
DOC_DIR="$SMV_TOOLS/../sphinx_docs"

DST=$1
[ -z $DST ] && echo "ERROR: destination not specified" && exit 1

if [ -z $SPARK_HOME ]; then
  SPARK_HOME="$(dirname $(which spark-submit))/.."
fi

[ -z $SPARK_HOME ] && echo "ERROR: can't find spark" && exit 1

export PYTHONPATH="$PKG_DIR:$PYTHONPATH"
# Need pyspark and py4j the sys.path so they can be imported by sphinx
export PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
export PYTHONPATH="$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH"

rm -rf $DOC_DIR
sphinx-apidoc --full -o $DOC_DIR $PKG_TO_DOC
cp $SMV_TOOLS/conf/sphinx-conf.py $DOC_DIR/conf.py
(cd $DOC_DIR; make html)

# maintain SMV gh-pages branch in its own directory
GHPAGES_DIR="$HOME/.smv.ghpages"
SMV_DIR="SMV"

mkdir -p $GHPAGES_DIR
cd $GHPAGES_DIR

# clone repo if it does not exist, else just pull
if [ ! -d $SMV_DIR ]; then
  git clone -b gh-pages https://github.com/TresAmigosSD/SMV.git
else
  (cd "$GHPAGES_DIR/$SMV_DIR"; git pull)
fi

# write the python docs directly to the SMV gh-pages branch
cd "$GHPAGES_DIR/$SMV_DIR"

mkdir -p $(dirname $DST)
cp -r $DOC_DIR/_build/html $DST
rm -rf $DOC_DIR
