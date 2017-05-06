#!/bin/bash

set -e

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
# TEMPLATE_DIR should have been defined and exported in the Dockerfile.
cp ${TEMPLATE_DIR}/bashrc ${USER_HOME}/.bashrc
mkdir -p ${USER_HOME}/.jupyter
cp ${TEMPLATE_DIR}/jupyter_notebook_config.py ${USER_HOME}/.jupyter/jupyter_notebook_config.py
chown -R ${USER_NAME}:${USER_NAME} ${USER_HOME}

# ensure /projects is also owned by SMV user.
chown -R ${USER_NAME}:${USER_NAME} /projects
cd /projects

if [[ $# == 0 ]]; then
    # start bash if user did not supply parameters.
    sudo -u ${USER_NAME} -i bash
elif [[ $1 == "--start-server" ]]; then
    SERVER_PROJ_DIR="${2:?must provide project directory}"
    # start smv server and jupyter server
    sudo -u ${USER_NAME} -i bash -c "
          cd ${SERVER_PROJ_DIR};
          (smv-jupyter&);
          smv-server"
else
    # start command supplied by user.
    sudo -u ${USER_NAME} -i "$@"
fi
