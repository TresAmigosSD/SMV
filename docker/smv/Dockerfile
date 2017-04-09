FROM tresamigos/smv-env:env_jdk7_py27

ENV SMV_HOME=/usr/lib/SMV \
    PATH=$PATH:/usr/lib/SMV/tools

RUN sudo apt-get update &&\
    sudo apt-get install -y "python-dev" "python-pip" &&\
    sudo pip install --upgrade pip &&\
    sudo pip install Flask &&\
    sudo pip install jupyter

# gave up on moving ivy dir using sbtopts.  Just link ~/.ivy2 to /projects/.ivy2
RUN wget https://github.com/TresAmigosSD/SMV/archive/master.zip &&\
    unzip master.zip &&\
    rm master.zip &&\
    sudo mv SMV-master $SMV_HOME/ &&\
    rm -rf /home/smv/.ivy2 &&\
    mkdir -p /projects/.ivy2 &&\
    ln -s /projects/.ivy2 /home/smv/.ivy2 &&\
    cd $SMV_HOME &&\
    sbt assembly publish-local &&\
    mvn install -DskipTests

RUN rm -rf /home/smv/.ivy2/cache/* &&\
    find /home/smv/.m2/repository -mindepth 1 \! -path '*tresamigos*' -exec rm -rf \{\} + &&\
    find $SMV_HOME/target/* -maxdepth 0 -not -path $SMV_HOME/target/scala-2.10 -exec rm -rf \{\} +

RUN sudo apt-get remove --purge -y python-dev &&\
    sudo apt-get remove --purge -y python-pip &&\
    sudo rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Configures maven repository to live on user's host mount (i.e. in /projects)
COPY ./settings.xml $M2_HOME/conf/settings.xml

COPY ./kernel.json /usr/local/share/jupyter/kernels/smv-pyshell/kernel.json
COPY ./.jupyter /home/smv/.jupyter
COPY ./hive-site.xml /usr/lib/spark/conf/hive-site.xml

RUN sudo chown -R smv:smv /home/smv/.jupyter &&\
    sudo chown -R smv:smv /usr/lib/spark/conf/hive-site.xml

# Entrypoint rsyncs maven and ivy caches from the container to the host
# mount, then executes user-supplied command or starts bash
COPY ./entrypoint.sh /usr/bin/entrypoint.sh

# Must use bracket syntax (["command"]) so that user can supply params (additional commands to execute)
ENTRYPOINT ["entrypoint.sh"]
