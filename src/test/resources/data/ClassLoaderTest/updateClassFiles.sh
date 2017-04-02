#!/bin/bash
# This should be run at the top level of the SMV project directory as follows:
#
# docker run --rm -it -v `pwd`/src/test/resources/data/ClassLoaderTest:/projects/cltest tresamigos/smv /bin/bash /projects/cltest/updateClassFiles.sh
#

if [ ! -f /.dockerenv ]; then
    echo "ERROR: must run inside SMV docker image.  See top of script"
    exit 1
fi

# download scala compiler (no need to add it to SMV image for just this case)
echo "----Downloading scalac"
cd /tmp
wget http://downloads.lightbend.com/scala/2.10.6/scala-2.10.6.tgz
tar xf scala-2.10.6.tgz

# rebuild the class files
echo "----Rebuild class files"
cd /projects/cltest
rm -f com/smv/*.class
/tmp/scala-2.10.6/bin/scalac -cp /usr/lib/SMV/target/scala-2.10/smv-1.5-SNAPSHOT-jar-with-dependencies.jar:/usr/lib/spark/lib/spark-assembly-1.5.2-hadoop2.7.2.jar src/com/smv/*.scala
