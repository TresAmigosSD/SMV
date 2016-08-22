#/bin/bash


#Expect that: 
#   $1 is SMV source directory
#   $2 is projects directory


VOL_STR=""

if [ -z ${M2_REPO+x} ]; then
    echo "Defaulting to mvn repository ~/.m2"
    M2_REPO=$(readlink -f ~/.m2)
fi

if [ -z ${IVY_REPO+x} ]; then
    echo "Defaulting to ivy repository ~/.ivy2"
    IVY_REPO=$(readlink -f ~/.ivy2)
fi

if ! [ -z ${1+x} ]; then
    VOL_STR="$VOL_STR -v $(readlink -f $1):/SMV"
fi

if ! [ -z ${2+x} ]; then
    VOL_STR="$VOL_STR -v $(readlink -f $2):/projects" 
fi

VOL_STR="$VOL_STR -v $M2_REPO:/home/smv/.m2 -v $IVY_REPO:/home/smv/.ivy2"

docker run --rm -it $VOL_STR smv-core