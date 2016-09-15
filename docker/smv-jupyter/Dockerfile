FROM tresamigos/smv

ARG BUILD_DEPS_APT="python-pip"

ENV PYSPARK_DRIVE_PYTHON="jupyter" \
    IPYTHON_OPTS="notebook --ip=*"

RUN sudo apt-get update &&\
    sudo apt-get install -y $BUILD_DEPS_APT &&\
    sudo pip install --upgrade pip &&\
    sudo pip install jupyter &&\
    sudo apt-get remove --purge -y $BUILD_DEPS_APT &&\
    sudo rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENTRYPOINT $SMV_HOME/tools/smv-pyshell