#/bin/bash
# Release the current version of SMV.  This will use the tresamigos:smv
# docker container to maintain release consistency.

# TODO: create github release automatically.
# TODO: use .smv_version file to get current version
# TODO: create /tmp/vx.x.x.x dir for logs and assets
# TODO: add "info" func to put message to stdout and logs
# TODO: redirect output of intermediate results to logs instead of stdout.
# TODO: verify that a current tag for the version does not alrady exist.

set -e
PROG_NAME=$(basename "$0")
ORIG_DIR=$(pwd)
SMV_TOOLS="$(cd "`dirname "$0"`"; pwd)"
SMV_DIR="$(dirname "$SMV_TOOLS")"
SMV_DIR_BASE="$(basename $SMV_DIR)"
DOCKER_SMV_DIR="/projects/${SMV_DIR_BASE}" # SMV dir inside the docker image.
PROJ_DIR="$(dirname "$SMV_DIR")" # assume parent of SMV directory is the projects dir.

function info()
{
  echo "---- $@"
}

function error()
{
  echo "ERROR: $@"
  exit 1
}

function usage()
{
  echo "USAGE: ${PROG_NAME} smv_version_to_release(a.b.c.d)"
  exit $1
}

function parse_args()
{
  info "parsing command line args"
  [ "$1" = "-h" ] && usage 0
  [ $# -ne 1 ] && echo "ERROR: invalid number of arguments" && usage 1

  SMV_VERSION="$1"
  validate_version "$SMV_VERSION"
}

function get_prev_smv_version()
{
  PREV_SMV_VERSION=$(cat "${SMV_DIR}/.smv_version")
  info "previous SMV version: $PREV_SMV_VERSION"
  validate_version "$PREV_SMV_VERSION"
}

# make sure version is of the format a.b.c.d where a,b,c,d are all numbers.
function validate_version()
{
  local ver="$1"
  local res=$(echo "$ver" | sed -E -e 's/^([0-9]+\.){3}[0-9]+$//')
  if [ -n "$res" ]; then
    echo "ERROR: invalid version format: $ver"
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
  info "find gnu tar"
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

# find the release message in /releases dir.
function find_release_msg_file()
{
  info "finding release message file"
  RELEASE_MSG_FILE="releases/v${SMV_VERSION}.md"
  cd "${SMV_DIR}"
  if [ ! -r "${RELEASE_MSG_FILE}" ]; then
    error "Unable to find release message file: ${RELEASE_MSG_FILE}"
  fi
}

function check_git_repo()
{
  echo "--- checking repo for modified files"
  cd "${SMV_DIR}"
  if ! git diff-index --quiet HEAD --; then
    error "SMV git repo has locally modified files"
  fi
}

function update_docs_version()
{
  info "updating docs to version $SMV_VERSION"
  cd "${SMV_DIR}"
  git pull # update to latest before making any changes.
  find docs/user -name '*.md' \
    -exec perl -pi -e "s/${PREV_SMV_VERSION}/${SMV_VERSION}/g" \{\} +
  git commit -a -m "updated user docs to version $SMV_VERSION"
  git push origin
}

function tag_release()
{
  local tag=v"$SMV_VERSION"
  info "tagging release as $tag"
  cd "${SMV_DIR}"
  git tag -a $tag -m "SMV Release $SMV_VERSION on `date +%m/%d/%Y`"
  git push origin $tag
}

function create_tar()
{
  echo "--- create tar image: "
  cd "$ORIG_DIR"

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
get_prev_smv_version
find_gnu_tar
find_release_msg_file
# check_git_repo
# build_smv
# update_docs_version
# tag_release
# create_tar
