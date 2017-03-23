#/bin/bash
# Release the current version of SMV.  This must be run from within the tresamigos:smv
# docker container to maintain release consistency

# TODO: make sure current repo doesn't have any checkout.
# TODO: add automatic taging of source


set -e
PROG_NAME=$(basename "$0")
SMV_TOOLS="$(cd "`dirname "$0"`"; pwd)"
SMV_DIR="$(dirname "$SMV_TOOLS")"
SMV_DIR_BASE="$(basename $SMV_DIR)"
DOCKER_SMV_DIR="/projects/${SMV_DIR_BASE}"
PROJ_DIR="$(dirname "$SMV_DIR")" # assume parent of SMV directory is the projects dir.

function usage()
{
  echo "USAGE: ${PROG_NAME} smv_version_to_release(n.n.n.n)"
  exit $1
}

function parse_args()
{
  [ "$1" = "-h" ] && usage 0
  [ $# -ne 1 ] && echo "ERROR: Missing arguments" && usage 1

  SMV_VERSION="$1"
  validate_version
}

# make sure version of the format a.b.c.d where a,b,c,d are all numbers.
function validate_version()
{
  local res=$(echo "$SMV_VERSION" | sed -E -e 's/^([0-9]+\.){3}[0-9]+$//')
  if [ -n "$res" ]; then
    echo "ERROR: invalid version format"
    usage 1
  fi
}

function build_smv()
{
  echo "--- Building SMV"
  # explicitly add -ivy flag as SMV docker image is not picking up sbtopts file. (SMV issue #556)
  docker run --rm -it -v ${PROJ_DIR}:/projects tresamigos/smv:latest \
    sh -c "cd $DOCKER_SMV_DIR; sbt -ivy /projects/.ivy2 clean assembly"
}

# find the gnu tar on this system.
function find_gnu_tar()
{
  echo "---- find gnu tar"
  local tars="gtar gnutar tar"
  TAR=""
  for t in $tars; do
    if [ -n "$(type -p $t)" ]; then
      TAR=$t
      break
    fi
  done

  # make sure it is gnu tar:
  if [ $($TAR --version | head -1 | grep "GNU tar" | wc -l) -ne 1 ]; then
    echo "ERROR: did not find a gnu tar.  Need gnu tar to build SMV release"
    exit 1
  fi
}

function create_tar()
{
  echo "--- create tar image: "
  echo "DOCKER_SMV_DIR=$DOCKER_SMV_DIR"

  # cleanup some unneeded binary files.
  rm -rf "${SMV_DIR}/project/target" "${SMV_DIR}/project/project"
  rm -rf "${SMV_DIR}/target/resolution-cache" "${SMV_DIR}/target/streams"
  find "${SMV_DIR}/target" -name '*with-dependencies.jar' -prune -o -type f -exec rm -f \{\} +

  # add the smv version to the SMV directory.
  echo ${SMV_VERSION} > "${SMV_DIR}/.smv_version"

  # create the tar image
  ${TAR} zcf ./smv_${SMV_VERSION}.tgz -C "${PROJ_DIR}" --exclude=.git --transform "s/^${SMV_DIR_BASE}/SMV_${SMV_VERSION}/" ${SMV_DIR_BASE}
}

# ---- MAIN ----
parse_args "$@"
find_gnu_tar
build_smv
create_tar
