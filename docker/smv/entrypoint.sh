#!/bin/bash

function start_server() {
    # ${PROJECT_DIR} is the pre-built project path name, "MyApp" by default
    if [ -z ${PROJECT_DIR+x} ]; then
        echo ">> No project defined. Start to use sample app..."
        PROJECT_DIR="MyApp"
    fi
    cd /projects/${PROJECT_DIR}
    smv-jupyter &
    smv-server
}

# if no params supplied, start bash
if [[ $# == 0 ]]; then
    bash
elif [[ $1 == "--start-server" ]]; then
    start_server
# if params supplied, execute them as a command
else
    "$@"
fi
