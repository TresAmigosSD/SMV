FROM openjdk:8-jdk

ARG DEBIAN_FRONTEND=noninteractive
ARG SBT_VERSION=0.13.15
ARG PYTHON_VERSION=2.7.13

ENV PYENV_ROOT=/.pyenv
ENV PATH=/usr/lib/spark/bin:/usr/lib/sbt/bin:${PATH}

COPY ./requirements.txt /artifacts/requirements.txt

RUN echo "deb http://cran.rstudio.com/bin/linux/debian lenny-cran/" >> /etc/apt/source.list &&\
    apt-key adv --keyserver hkp://pgp.mit.edu --recv-key 381BA480 &&\
    apt-get update &&\
    apt-get install -y r-base &&\
    apt-get install -y sudo &&\
    apt-get install -y vim &&\
    apt-get install -y git &&\
    apt-get install -y graphviz &&\
    apt-get install -y libssl-dev

# install sbt.  The real sbt will be downloaded the first time sbt is run.
# Note: do not run "sbt exit" here to prime the download as we want to wait until user mounts /projects/.ivy2
RUN wget https://dl.bintray.com/sbt/native-packages/sbt/$SBT_VERSION/sbt-$SBT_VERSION.tgz &&\
    tar -xzvf sbt-$SBT_VERSION.tgz &&\
    mv sbt /usr/lib/sbt &&\
    rm sbt-$SBT_VERSION.tgz

# Debian repositories don't have all of the supported versions of Python, so it will be easiest to
# use pyenv to manage installation
RUN curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash && \
    ${PYENV_ROOT}/bin/pyenv install ${PYTHON_VERSION} &&\
    ${PYENV_ROOT}/bin/pyenv global ${PYTHON_VERSION} &&\
    ${PYENV_ROOT}/versions/${PYTHON_VERSION}/bin/pip install --upgrade pip && \
    ${PYENV_ROOT}/versions/${PYTHON_VERSION}/bin/pip install -r /artifacts/requirements.txt

# cleanup python dev tools and apt caches.
RUN apt-get clean &&\
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /artifacts/requirements.txt