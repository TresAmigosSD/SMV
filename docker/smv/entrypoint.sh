#!/bin/bash

# ensure that the mvn and ivy caches have been copied to the host mount
mkdir -p /projects/.ivy2 /projects/.m2
rsync -r /home/smv/.ivy2/* /projects/.ivy2
rsync -r /home/smv/.m2/* /projects/.m2

function start_server() {
    cd /projects
    if [ `ls | wc -l` = 0 ]; then
        # if no project is mounted, create a sample app and start the server
        smv-init MyApp com.mycompany.myproj
        cd MyApp/
        sbt assembly
        smv-server
    else
        # if a project is found, start the server directly
        # ${PROJECT_DIR} is the project path name
        cd "${PROJECT_DIR}"
        sbt assembly
        smv-server
    fi
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
