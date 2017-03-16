#/bin/bash
# Release the current version of SMV.  This must be run from within the tresamigos:smv
# docker container to maintain release consistency

SMV_VERSION="1.5.2.2"
SMV_SRC="/usr/lib/SMV"


if [ ! -f /.dockerenv ]; then
  echo "ERROR: this must be run from within the tresamigs:smv docker image"
  exit 1
fi

if [ ! -d /projects/release ]; then
  echo "ERROR: must have the /projects/release directory mounted"
  exit 1
fi

if [ ! -d /usr/lib/SMV/target/scala-2.10 ]; then
  echo "ERROR: can not find SMV version built with scala-2.10"
  exit 1
fi

echo "--- cleanup release"
cd /usr/lib/SMV/target/scala-2.10 
# only keep the full fat jar from SMV.  Nothing else is needed
ls -1 | grep -v "with-dependencies.jar" | xargs rm -rf

echo "--- creating a versioned release"
echo ${SMV_VERSION} > /usr/lib/SMV/version.txt


echo "--- creating tar image"
tar zcf /projects/release/smv_${SMV_VERSION}.tgz -C /usr/lib --transform "s/^SMV/SMV_${SMV_VERSION}/" SMV
