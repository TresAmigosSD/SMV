FROM tresamigos/smv-core

ENV SMV_HOME=/usr/lib/SMV \
    PATH=$PATH:/usr/lib/SMV/tools

RUN wget http://github.com/TresAmigosSD/SMV/archive/master.zip &&\
    unzip master.zip &&\
    rm master.zip &&\
    sudo mv SMV-master $SMV_HOME/ &&\
    cd $SMV_HOME &&\
    mvn clean install -DskipTests &&\
    sbt publish-local

#Custom mvn settings cofigure repository to live on user's host mount but also leverage cache from SMV build
COPY ./settings.xml $M2_HOME/conf/settings.xml

#Custom sbtops configures sbt to use ivy dependency and local caches on host mount
COPY ./sbtopts /home/smv/lib/sbt/conf/sbtopts

COPY ./entrypoint.sh /usr/bin/entrypoint.sh

#Must use bracket syntax (["command"]) so that user can supply params
ENTRYPOINT ["entrypoint.sh"]