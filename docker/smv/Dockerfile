FROM tresamigos/smv-core

ENV SMV_HOME=/usr/lib/SMV \
    PATH=$PATH:/usr/lib/SMV/tools

RUN wget http://github.com/TresAmigosSD/SMV/archive/master.zip &&\
    unzip master.zip &&\
    rm master.zip &&\
    sudo mv SMV-master $SMV_HOME/ &&\
    cd $SMV_HOME &&\
    mvn install -DskipTests &&\
    sbt publish-local

# Configures maven repository to live on user's host mount (i.e. in /projects)
COPY ./settings.xml $M2_HOME/conf/settings.xml

# Configures ivy (through sbt) repository to live on user's host mount (i.e. in /projects)
COPY ./sbtopts /home/smv/lib/sbt/conf/sbtopts

# Entrypoint rsyncs maven and ivy caches from the container to the host
# mount, then executes user-supplied command or starts bash
COPY ./entrypoint.sh /usr/bin/entrypoint.sh

# Must use bracket syntax (["command"]) so that user can supply params (additional commands to execute)
ENTRYPOINT ["entrypoint.sh"]