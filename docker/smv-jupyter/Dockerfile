FROM tresamigos/smv

ARG BUILD_DEPS_APT="python-pip"

RUN sudo apt-get update &&\
    sudo apt-get install -y $BUILD_DEPS_APT &&\
    sudo pip install --upgrade pip &&\
    sudo pip install jupyter &&\
    sudo apt-get remove --purge -y $BUILD_DEPS_APT &&\
    sudo rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY kernel.json /usr/local/share/jupyter/kernels/smv-pyshell/kernel.json

COPY entrypoint.sh /usr/bin/entrypoint.sh

RUN sudo chmod a+x /usr/bin/entrypoint.sh

WORKDIR /proj

ENTRYPOINT ["entrypoint.sh"] 