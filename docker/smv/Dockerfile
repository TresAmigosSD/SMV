FROM openjdk:7-jdk

ARG DEBIAN_FRONTEND=noninteractive
ENV PATH=$PATH:/usr/lib/SMV/tools:/usr/lib/spark/bin

RUN echo "deb http://cran.rstudio.com/bin/linux/debian lenny-cran/" >> /etc/apt/source.list &&\
    apt-key adv --keyserver hkp://pgp.mit.edu --recv-key 381BA480 &&\
    apt-get update &&\
    apt-get install -y r-base &&\
    apt-get install -y unzip &&\
    apt-get install -y sudo &&\
    apt-get install -y vim &&\
    apt-get install -y rsync &&\
    apt-get install -y git &&\
    apt-get install -y python "python-dev" "python-pip" &&\
    apt-get install -y graphviz

# install smv and spark and move them into /usr/lib dir.
RUN mkdir /projects &&\
    cd /projects &&\
    wget -qO- https://raw.githubusercontent.com/TresAmigosSD/SMV/master/tools/smv-install | bash -s -- -spark 1.5.2.5 &&\
    mkdir -p /usr/lib &&\
    mv SMV_* /usr/lib/SMV &&\
    mv spark-* /usr/lib/spark

# installs python packages.
# TODO: use requirements.txt from image downloaded above once 1.5.2.6 is out.
COPY ./requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt

# cleanup python dev tools and apt caches.
RUN apt-get remove --purge -y python-dev &&\
    apt-get remove --purge -y python-pip &&\
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY ./kernel.json /usr/local/share/jupyter/kernels/smv-pyshell/kernel.json
COPY ./hive-site.xml /usr/lib/spark/conf/hive-site.xml
COPY ./entrypoint.sh /usr/bin/entrypoint.sh

# copy files into template directory.  They will be copied to smv home later in entrypoint.sh
ENV TEMPLATE_DIR=/home/template
COPY ./bashrc ./jupyter_notebook_config.py ${TEMPLATE_DIR}/

# Must use bracket syntax (["command"]) so that user can supply params (additional commands to execute)
ENTRYPOINT ["entrypoint.sh"]
