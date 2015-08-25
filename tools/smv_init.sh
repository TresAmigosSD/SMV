#!/bin/bash

QUITE_MODE=0
if [ "$1" = "-q" ]; then
    QUITE_MODE=1
    shift
fi

if [ $# -ne 2 ]; then
    echo "ERROR: Invalid number of arguments"
    echo "USAGE: $0 [-q] project_name base_project_class"
    echo "example:"
    echo "  \$ $0 MyProject com.mycompany.myproj"
    echo ""
    echo "In the above example, the group id of the project will be 'com.mycompany'"
    echo "and the artifact id will be 'myproj'"
    exit 1
fi

TEMPLATE_DIR="$(cd "`dirname "$0"`"; pwd)/template"
PROJ_DIR="$1"
PROJ_CLASS="$2"

function extract_group_artifact_ids()
{
    PROJ_GROUP_ID="${PROJ_CLASS%.*}"
    PROJ_ARTIFACT_ID="${PROJ_CLASS##*.}"

    if [ "$PROJ_GROUP_ID" == "$PROJ_ARTIFACT_ID" ]; then
        echo "Invalid project class: $PROJ_CLASS"
        echo "the class must have at least two levels a.b"
        exit 1
    fi
}

function create_proj_dir()
{
    echo "-- creating project directory"

    if [ -d "$PROJ_DIR" ]; then
      echo "$PROJ_DIR already exists"
      exit 1
    fi

    mkdir "$PROJ_DIR"
}

function copy_with_inject()
{
    SRC="$1"
    DST="$2"
    echo "-- copying `basename "$DST"`"

    mkdir -p "$(dirname "$DST")"
    sed -e "s/_GROUP_ID_/$PROJ_GROUP_ID/" \
        -e "s/_ARTIFACT_ID_/$PROJ_ARTIFACT_ID/" \
        < "$SRC" > "$DST"
}

function copy_conf_files()
{
    FILES="pom.xml README.md log4j.properties conf/shell_init.scala conf/smv-app-conf.props"
    for f in $FILES; do
        copy_with_inject "$TEMPLATE_DIR/$f" "$PROJ_DIR/$f"
    done
}

function copy_data_files()
{
    echo "-- copying data files"

    cp -R "${TEMPLATE_DIR}/data" "$PROJ_DIR"
}

function copy_src_files()
{
    echo "-- copying source files"

    PROJ_CLASS_PATH="`echo $PROJ_CLASS | sed -e 's/\./\//g'`"
    STAGE1_PKG_PATH="${PROJ_DIR}/src/main/scala/${PROJ_CLASS_PATH}/stage1"

    # TODO: rename ExampleApp to Stage1InputSet.scala
    SRC_INPUT_SET="${TEMPLATE_DIR}/src/ExampleApp.scala"
    DST_INPUT_SET="${STAGE1_PKG_PATH}/Stage1InputSet.scala"
    copy_with_inject "$SRC_INPUT_SET" "$DST_INPUT_SET"

    SRC_EMPLOYMENT="${TEMPLATE_DIR}/src/Employment.scala"
    DST_EMPLOYMENT="${STAGE1_PKG_PATH}/etl/Employment.scala"
    copy_with_inject "$SRC_EMPLOYMENT" "$DST_EMPLOYMENT"
}

# --- MAIN ---
extract_group_artifact_ids
create_proj_dir
copy_conf_files
copy_data_files
copy_src_files

