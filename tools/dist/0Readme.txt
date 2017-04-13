This directory contains a sample makedist.sh script for creating a distribution
bundle.  It was extracted from a project that had scala code and was built
with maven.  For python projects that do not have scala code, this needs to be
modified to use the SMV fat jar instead of the project fat jar (and skip the build
step alltogether)


The result of the makedist call is a bundle file that can be just unzipped
and run on a production server.
