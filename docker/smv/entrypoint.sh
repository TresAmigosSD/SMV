#!/bin/bash

# ensure that the mvn and ivy caches have been copied to the host mount
mkdir -p /projects/.ivy2 /projects/.m2
rsync -r /home/smv/.ivy2/* /projects/.ivy2
rsync -r /home/smv/.m2/* /projects/.m2


# if no params supplied, start bash
if [[ $# == 0 ]]; then
    bash
# if params supplied, execute them as a command
else
    "$@"
fi