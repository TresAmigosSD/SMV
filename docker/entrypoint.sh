#!/bin/bash

mkdir -p /projects/.ivy2 /projects/.m2
rsync -r /home/smv/.ivy2/* /projects/.ivy2
rsync -r /home/smv/.m2/* /projects/.m2

if [[ $# == 0 ]]; then
    bash
else
    "$@"
fi