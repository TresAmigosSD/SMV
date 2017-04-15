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

# Add smv user (must be done after install sudo).
RUN useradd -G sudo --create-home --shell /bin/bash smv &&\
    echo "smv ALL = NOPASSWD: ALL" >> /etc/sudoers &&\
    mkdir /projects &&\
    chown smv:smv /projects

# installs python packages.
COPY ./requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt

# install smv and spark and move them into /usr/lib dir.
RUN cd /projects &&\
  wget -qO- https://raw.githubusercontent.com/TresAmigosSD/SMV/master/tools/smv-install | bash -s -- -spark 1.5.2.5 &&\
  mkdir -p /usr/lib &&\
  mv SMV_* /usr/lib/SMV &&\
  mv spark-* /usr/lib/spark

# cleanup python dev tools and apt caches.
RUN apt-get remove --purge -y python-dev &&\
    apt-get remove --purge -y python-pip &&\
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# switch to smv user
WORKDIR /projects
USER smv

# gave up on moving ivy dir using sbtopts.  Just link ~/.ivy2 to /projects/.ivy2
RUN rm -rf /home/smv/.ivy2 &&\
    mkdir -p /projects/.ivy2 &&\
    ln -s /projects/.ivy2 /home/smv/.ivy2 &&\
    rm -rf /home/smv/.ivy2/cache/* &&\
    mkdir /home/smv/.jupyter

COPY ./bashrc /home/smv/.bashrc
COPY ./kernel.json /usr/local/share/jupyter/kernels/smv-pyshell/kernel.json
COPY ./jupyter_notebook_config.py /home/smv/.jupyter/jupyter_notebook_config.py
COPY ./hive-site.xml /usr/lib/spark/conf/hive-site.xml
COPY ./entrypoint.sh /usr/bin/entrypoint.sh

RUN sudo chown -R smv:smv /home/smv/.jupyter &&\
    sudo chown -R root:root /usr/lib/spark/conf/hive-site.xml

# Must use bracket syntax (["command"]) so that user can supply params (additional commands to execute)
ENTRYPOINT ["entrypoint.sh"]
