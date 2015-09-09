#!/bin/bash

# This used https://github.com/sindresorhus/generate-github-markdown-css to get the github.css file

function verify_pandoc()
{
    type -t pandoc >/dev/null || (echo "ERROR: Can not find 'pandoc' in path"; exit 1)
}

function gen_user_guide()
{
    echo "-- Generating user documentation in $OUTPUT_DIR"
    find docs/user -name '*.md' -print | while read MD; do
        local HTML="${OUTPUT_DIR}/$(echo $MD | sed -e 's/\.md$/.html/' -e 's/^docs\///')"
        mkdir -p $(dirname "$HTML")
        pandoc -s -f markdown_github -t html \
            "--include-in-header=${SMV_HOME}/tools/conf/docs/header.html" \
            "--include-before-body=${SMV_HOME}/tools/conf/docs/body_before.html" \
            "--include-after-body=${SMV_HOME}/tools/conf/docs/body_after.html" \
            -o - "${MD}" |\
        sed -e 's/\(href="[^"]*\)\.md"/\1.html"/g' |\
        cat > "${HTML}"
    done

    cp "${SMV_HOME}/tools/conf/docs/github.css" "${OUTPUT_DIR}/user"
}

SMV_HOME="$(cd "`dirname "$0"`"/..; pwd)"
OUTPUT_DIR=target/_site

verify_pandoc
gen_user_guide

