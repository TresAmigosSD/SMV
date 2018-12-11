#############################################################################################################
# SMV Dockerfile
#
# Usage: build the SMV docker image with `make docker` - the rest of this is for the release process
#
# SMV utilizes a multistage Docker build to integrate the process of building and testing new release
# artifacts with the process of building new Docker images. Stage 1 is exclusively for generating the release
# artifact. It installs dev dependencies, compiles SMV, and runs the local_bundle target from the Makefile.
# The second stage installs only the production dependencies and then copies and unpacks the release artifact
# from the first stage. This approach typically reduces image size, although at this time SMV's production  
# dependencies so closely match its dev dependencies that there may not be much of a delta. The first stage
# is also used as the canonical build environment for the release - the release copies the artifact out of
# the image and publishes that to Github. 
#############################################################################################################

#############################################################################################################
# 1. SMV builder image
#############################################################################################################
FROM openjdk:8-jdk as smv-build

ARG DEBIAN_FRONTEND=noninteractive
ARG SBT_VERSION=0.13.15
ENV PYENV_ROOT=/.pyenv
ENV SMV_HOME=/usr/lib/SMV
ENV SBT_HOME=/usr/lib/sbt
ENV PATH=${SBT_HOME}/bin:${PYENV_ROOT}/bin:${PATH}
RUN apt-get -y update

RUN wget https://dl.bintray.com/sbt/native-packages/sbt/$SBT_VERSION/sbt-$SBT_VERSION.tgz
RUN tar -xzvf sbt-$SBT_VERSION.tgz
RUN mv sbt /usr/lib/sbt
RUN rm sbt-$SBT_VERSION.tgz

RUN apt-get -y install make gcc libssl-dev zlib1g-dev

COPY . ${SMV_HOME}
WORKDIR ${SMV_HOME}

RUN cd ${SMV_HOME} && ls && make clean 
RUN cd ${SMV_HOME} && make assemble-fat-jar 
RUN cd ${SMV_HOME} && make local_bundle

ARG PYTHON_VERSION=2.7.13
# Debian repositories don't have all of the supported versions of Python, so it will be easiest to
# use pyenv to manage installation
RUN curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
RUN ${PYENV_ROOT}/bin/pyenv install ${PYTHON_VERSION}
RUN ${PYENV_ROOT}/bin/pyenv global ${PYTHON_VERSION} 
RUN ${PYENV_ROOT}/versions/${PYTHON_VERSION}/bin/pip install --upgrade pip
RUN ${PYENV_ROOT}/versions/${PYTHON_VERSION}/bin/pip install -r ${SMV_HOME}/docker/smv/requirements.txt

#############################################################################################################
# 2. SMV publishable image
#############################################################################################################
FROM openjdk:8-jdk as smv

ENV TEMPLATE_DIR=/home/template
ENV PYENV_ROOT=/.pyenv
ENV SMV_HOME=/usr/lib/SMV
ENV SPARK_HOME=/usr/lib/spark
ENV PATH=${SPARK_HOME}/bin:${SMV_HOME}/tools:${PYENV_ROOT}/bin/:${PATH}

RUN apt-get -y update &&\
    apt-get -y install sudo git vim graphviz

# create the projects directory and create a flag (.docker) to indicate project is inside docker image.
RUN mkdir /projects &&\
    touch /projects/.docker &&\
    mkdir ${TEMPLATE_DIR}

# Copy the pyenv installation from the first stage. This is an optimization so we don't install Python twice.
# we can get away with this because we don't currently differentiate between dev and prod requirements.
COPY --from=smv-build ${PYENV_ROOT} ${PYENV_ROOT}

# Copy the SMV release artifact from the first stage and unpack it. Since the same artifact is also copied
# out and published as the official release, we are guaranteed that the published image matches the published
# This works around the chicken/egg issues encountered with the old approach of downloading a version of 
# artifact. `smv-install` from Github raw content to install (probably) the most recent release.
COPY --from=smv-build /usr/lib/SMV/smv_*.tgz .
RUN tar xzvf smv_*.tgz && rm -rf smv_*.tgz && mv ./SMV ${SMV_HOME} &&\
    ${SMV_HOME}/tools/spark-install --target-dir ${SPARK_HOME} &&\
    mkdir -p /usr/local/share/jupyter/kernels/smv-pyshell/ &&\
    cp ${SMV_HOME}/docker/smv/kernel.json /usr/local/share/jupyter/kernels/smv-pyshell/kernel.json &&\
    cp ${SMV_HOME}/docker/smv/hive-site.xml /usr/lib/spark/conf/hive-site.xml &&\
    cp ${SMV_HOME}/docker/smv/entrypoint.sh /usr/bin/entrypoint.sh &&\
    cp ${SMV_HOME}/docker/smv/bashrc /usr/lib/SMV/docker/smv/jupyter_notebook_config.py ${TEMPLATE_DIR}/

# Must use bracket syntax (["command"]) so that user can supply params (additional commands to execute)
ENTRYPOINT ["entrypoint.sh"]
