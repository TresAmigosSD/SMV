#/bin/bash


#Expect that: 
#   $1 is SMV source directory
#   $2 is projects directory


VOL_STR=""

if [ -z ${M2_REPO+x} ]; then
    M2_REPO=$(readlink -f ~/.m2)
fi

echo "Mounting mvn repository from $M2_REPO"


if [ -z ${IVY_REPO+x} ]; then
    IVY_REPO=$(readlink -f ~/.ivy2)
fi

echo "Mounting ivy repository from $IVY_REPO"


if ! [ -z ${1+x} ]; then
    PROJECTS_DIR="$(readlink -f $1)"
    echo "Mounting projects directory from $PROJECTS_DIR"
    VOL_STR="$VOL_STR -v $PROJECTS_DIR:/projects" 
fi


if [ -z ${2+x} ]; then
   SMV_DIR=$(readlink -f ../..)
else
   SMV_DIR=$(readlink -f $2)
fi

echo "Mounting SMV directory from $SMV_DIR"


VOL_STR="$VOL_STR -v $M2_REPO:/home/smv/.m2 -v $IVY_REPO:/home/smv/.ivy2 -v $SMV_DIR:/SMV"


docker run --rm -it $VOL_STR smv-core
