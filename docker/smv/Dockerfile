#############################################################################################################
# SMV builder
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
# SMV publishable
#############################################################################################################
FROM openjdk:8-jdk as smv

ENV TEMPLATE_DIR=/home/template

# TODO: does the docker image really need git or r-base any more?
RUN echo "deb http://cran.rstudio.com/bin/linux/debian lenny-cran/" >> /etc/apt/source.list &&\
    apt-key adv --keyserver hkp://pgp.mit.edu --recv-key 381BA480 &&\
    apt-get update &&\
    apt-get install -y r-base &&\ 
    apt-get install -y sudo &&\
    apt-get install -y vim &&\
    apt-get install -y git &&\
    apt-get install -y graphviz

ENV PYENV_ROOT=/.pyenv
ENV SMV_HOME=/usr/lib/SMV
ENV SPARK_HOME=/usr/lib/spark
ENV PATH=${SPARK_HOME}/bin:${SMV_HOME}/tools:${PYENV_ROOT}/bin/:${PATH}

# create the projects directory and create a flag (.docker) to indicate project is inside docker image.
RUN mkdir /projects &&\
    touch /projects/.docker &&\
    mkdir ${TEMPLATE_DIR}

COPY --from=smv-build ${PYENV_ROOT} ${PYENV_ROOT}
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
