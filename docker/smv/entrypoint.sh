#!/bin/bash

# ensure that the mvn and ivy caches have been copied to the host mount
mkdir -p /projects/.ivy2 /projects/.m2
rsync -r /home/smv/.ivy2/* /projects/.ivy2
rsync -r /home/smv/.m2/* /projects/.m2

function start_server() {
    # ${PROJECT_DIR} is the pre-built project path name, "MyApp" by default
    if [ -z ${PROJECT_DIR+x} ]; then
        echo ">> No project defined. Start to use sample app..."
        PROJECT_DIR="MyApp"
    fi
    cd /projects/${PROJECT_DIR}
    smv-server &
    smv-jupyter &
}

# if no params supplied, start bash
if [[ $# == 0 ]]; then
    bash
elif [[ $1 == "--server" ]]; then
    start_server
# if params supplied, execute them as a command
else
    "$@"
fi
