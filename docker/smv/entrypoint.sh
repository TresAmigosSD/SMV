#!/bin/bash

set -e

function start_server() {
    # TODO: this should be an argument to start-server instead of an environment variable.
    # ${PROJECT_DIR} is the pre-built project path name, "MyApp" by default
    if [ -z ${PROJECT_DIR+x} ]; then
        echo ">> No project defined. Start to use sample app..."
        PROJECT_DIR="MyApp"
    fi
    cd /projects/${PROJECT_DIR}
    smv-jupyter &
    smv-server
}

# allow user to override smv user id when running image.
USER_ID=1000
if [[ "$1" == "-u" ]]; then
    USER_ID="$2"
    shift; shift
fi
USER_NAME=smv
USER_HOME=/home/${USER_NAME}

# add group/user (should fail if user/group already exists)
groupadd -g $USER_ID $USER_NAME
useradd -u $USER_ID -g $USER_ID -G sudo --create-home --home-dir ${USER_HOME} --shell /bin/bash $USER_NAME
echo "$USER_NAME ALL = NOPASSWD: ALL" >> /etc/sudoers

# gave up on moving ivy dir using sbtopts.  Just link ~/.ivy2 to /projects/.ivy2
mkdir -p /projects/.ivy2
rm -rf ${USER_HOME}/.ivy2
ln -s /projects/.ivy2 ${USER_HOME}/.ivy2

# copy bashrc / jupyter config into new user dir and make user own them.
cp ${TEMPLATE_DIR}/bashrc ${USER_HOME}/.bashrc
mkdir -p ${USER_HOME}/.jupyter
cp ${TEMPLATE_DIR}/jupyter_notebook_config.py ${USER_HOME}/.jupyter/jupyter_notebook_config.py
chown -R ${USER_NAME}:${USER_NAME} ${USER_HOME}

# ensure /projects is also owned by SMV user.
chown -R ${USER_NAME}:${USER_NAME} /projects
cd /projects

if [[ $# == 0 ]]; then
    # start bash if user did not supply parameters.
    sudo -u ${USER_NAME} bash
elif [[ $1 == "--start-server" ]]; then
    # start smv server and jupyter server
    sudo -u ${USER_NAME} start_server // TODO: may need to move this to outside script
else
    # start command supplied by user.
    sudo -u ${USER_NAME} "$@"
fi
