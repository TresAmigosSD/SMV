#!/bin/bash


if [ "$#" -ne 2 ]; then
  echo "ERROR: Invalid number of arguments"
  echo "USAGE: $0 current_version target_version"

  echo "example:"
  echo "  \$ $0 1.31 1.32"
  exit 1
fi

FROM_VERSION=$1
TO_VERSION=$2

OUTPUT_DIR="/tmp/docgen"


function get_latest_ghpages()
{
  # maintain SMV gh-pages branch in its own directory
  GHPAGES_DIR="$HOME/.smv/ghpages"
  SMV_DIR="SMV"

  mkdir -p $GHPAGES_DIR
  cd $GHPAGES_DIR

  echo "-- fetching latest SMV gh-pages branch to ~/.smv/ghpages/SMV ..."
  # clone repo if it does not exist, else just pull
  if [ ! -d $SMV_DIR ]; then
    git clone -b gh-pages https://github.com/TresAmigosSD/SMV.git
  else
    (cd "$GHPAGES_DIR/$SMV_DIR"; git pull)
  fi
}

function write_docs()
{
  # write the python docs directly to the SMV gh-pages branch
  cd "$GHPAGES_DIR/$SMV_DIR"

  PYVERSION_DIR="pythondocs/$TO_VERSION"
  SCALAVERSION_DIR="scaladocs/$TO_VERSION"

  PYDOC_DIR="$OUTPUT_DIR/pythondocs"
  SCALADOC_DIR="$OUTPUT_DIR/scaladocs"

  echo "-- copying scaladocs to ~/.smv/ghpages/SMV/scaladocs ..."
  echo "-- copying pythondocs to ~/.smv/ghpages/SMV/pydocs ..."
  # put the docs in the right version subdirectory
  mkdir -p $PYVERSION_DIR
  mkdir -p $SCALAVERSION_DIR
  cp -R ${PYDOC_DIR}/_build/html/* $PYVERSION_DIR
  cp -R ${SCALADOC_DIR}/* $SCALAVERSION_DIR
  rm -rf $OUTPUT_DIR
}

function update_links()
{
  echo "-- replacing version numbers in index.html ..."
  sed -i '.bak' "s/${FROM_VERSION}/${TO_VERSION}/g" index.html

  # get rid of sed backup file
  rm index.html.bak
}

function commit_push()
{
  echo "-- commiting and pushing doc updates to github SMV/gh-pages..."
  git add .
  git commit -a -m "updating docs from v${FROM_VERSION} to v${TO_VERSION}"
  git push
}

./admin/gen_docs.sh $OUTPUT_DIR
get_latest_ghpages
write_docs
update_links
commit_push
