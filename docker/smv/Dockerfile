FROM openjdk:8-jdk

ARG DEBIAN_FRONTEND=noninteractive
ARG SBT_VERSION=0.13.15
ARG PYTHON_VERSION=2.7.13

ENV TEMPLATE_DIR=/home/template

# create the projects directory and create a flag (.docker) to indicate project is inside docker image.
RUN mkdir /projects &&\
    touch /projects/.docker &&\
    mkdir ${TEMPLATE_DIR}

RUN echo "deb http://cran.rstudio.com/bin/linux/debian lenny-cran/" >> /etc/apt/source.list &&\
    apt-key adv --keyserver hkp://pgp.mit.edu --recv-key 381BA480 &&\
    apt-get update &&\
    apt-get install -y r-base &&\
    apt-get install -y unzip &&\
    apt-get install -y sudo &&\
    apt-get install -y vim &&\
    apt-get install -y rsync &&\
    apt-get install -y git &&\
    apt-get install -y bc &&\
    apt-get install -y graphviz &&\
    apt-get install -y libssl-dev

# install sbt.  The real sbt will be downloaded the first time sbt is run.
# Note: do not run "sbt exit" here to prime the download as we want to wait until user mounts /projects/.ivy2
RUN cd /projects &&\
    wget https://dl.bintray.com/sbt/native-packages/sbt/$SBT_VERSION/sbt-$SBT_VERSION.tgz &&\
    tar -xzvf sbt-$SBT_VERSION.tgz &&\
    mv sbt /usr/lib/sbt &&\
    rm sbt-$SBT_VERSION.tgz

# install smv and spark and move them into /usr/lib dir.
RUN cd /projects &&\
    wget -qO- https://raw.githubusercontent.com/TresAmigosSD/SMV/2/tools/smv-install | bash -s -- -spark &&\
    mkdir -p /usr/lib &&\
    mv SMV_* /usr/lib/SMV &&\
    mv spark-* /usr/lib/spark

# Debian repositories don't have all of the supported versions of Python, so it will be easiest to
# use pyenv to manage installation
RUN curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash && \
    /root/.pyenv/bin/pyenv install ${PYTHON_VERSION} &&\
    /root/.pyenv/bin/pyenv global ${PYTHON_VERSION} &&\
    /root/.pyenv/versions/${PYTHON_VERSION}/bin/pip install --upgrade pip && \
    /root/.pyenv/versions/${PYTHON_VERSION}/bin/pip install -r /usr/lib/SMV/docker/smv/requirements.txt && \
    cp -r /root/.pyenv ${TEMPLATE_DIR}/.pyenv

# cleanup python dev tools and apt caches.
RUN apt-get clean &&\
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY ./kernel.json /usr/local/share/jupyter/kernels/smv-pyshell/kernel.json
COPY ./hive-site.xml /usr/lib/spark/conf/hive-site.xml
COPY ./entrypoint.sh /usr/bin/entrypoint.sh

# copy files into template directory.  They will be copied to smv home later in entrypoint.sh
COPY ./bashrc ./jupyter_notebook_config.py ${TEMPLATE_DIR}/

# Must use bracket syntax (["command"]) so that user can supply params (additional commands to execute)
ENTRYPOINT ["entrypoint.sh"]
