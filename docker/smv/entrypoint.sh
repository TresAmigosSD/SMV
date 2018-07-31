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
rm -rf ${USER_HOME}/.ivy2
ln -s /projects/.ivy2 ${USER_HOME}/.ivy2

# copy bashrc / jupyter config into new user dir and make user own them.
# TEMPLATE_DIR should have been defined and exported in the Dockerfile.
cp ${TEMPLATE_DIR}/bashrc ${USER_HOME}/.bashrc
mkdir -p ${USER_HOME}/.jupyter
cp ${TEMPLATE_DIR}/jupyter_notebook_config.py ${USER_HOME}/.jupyter/jupyter_notebook_config.py
chown -R ${USER_NAME}:${USER_NAME} ${USER_HOME} ${PYENV_ROOT}

# ensure /projects is also owned by SMV user if it was not mounted by user.
if [ -f /projects/.docker ]; then
  chown -R ${USER_NAME}:${USER_NAME} /projects
fi
# We don't want to chown this directory as it could be quite large. Instead, if
# we do create it we will create it under USER_NAME instea
sudo -u ${USER_NAME} -i bash -c "mkdir -p /projects/.ivy2"
cd /projects

E_EXTENDED_SMV_SERVER_SCRIPT=''

if [[ $# == 0 ]]; then
    # start bash if user did not supply parameters.
    sudo -u ${USER_NAME} -i bash
elif [[ $1 == "--start-server" ]]; then
    SERVER_PROJ_DIR="${2:?must provide project directory}"
    if [[ "$3" == "-e" ]]; then
      E_EXTENDED_SMV_SERVER_SCRIPT="-e ${4:?must provide extended smv server script fullname}"
    fi

    # start smv server and jupyter server
    sudo -u ${USER_NAME} -i bash -c "
          cd ${SERVER_PROJ_DIR};

          (smv-jupyter \
            --ip ${JUPYTER_IP:?error JUPYTER_IP not set} \
            --port ${JUPYTER_PORT:?error JUPYTER_PORT not set} &);

          smv-server ${E_EXTENDED_SMV_SERVER_SCRIPT}\
            --ip ${SMV_IP:?error SMV_IP not set} \
            --port ${SMV_PORT:?error SMV_PORT not set}"
else
    # start command supplied by user.
    sudo -u ${USER_NAME} -i "$@"
fi
