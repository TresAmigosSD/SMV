FROM openjdk:7-jdk

ARG LIB_DIR=/usr/lib

ENV M2_HOME=$LIB_DIR/mvn \
    SPARK_HOME=$LIB_DIR/spark \
    SBT_HOME=$LIB_DIR/sbt \
    SBT_VERSION=0.13.11 \
    MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m" \
    MVN_VERSION=3.3.9

ENV PATH=$PATH:$M2_HOME/bin:$SPARK_HOME/bin:$SBT_HOME/bin

ARG DEBIAN_FRONTEND=noninteractive

RUN echo "deb http://cran.rstudio.com/bin/linux/debian lenny-cran/" >> /etc/apt/source.list &&\
    apt-key adv --keyserver hkp://pgp.mit.edu --recv-key 381BA480 &&\
    apt-get update &&\
    apt-get install -y r-base &&\
    apt-get install -y unzip &&\
    apt-get install -y sudo &&\
    apt-get install -y vim &&\
    apt-get install -y rsync &&\
    apt-get install -y git &&\
    apt-get install -y python &&\
    apt-get install -y graphviz &&\
    \
    mkdir -p $LIB_DIR &&\
    \
    wget http://www-us.apache.org/dist/maven/maven-3/$MVN_VERSION/binaries/apache-maven-$MVN_VERSION-bin.tar.gz &&\
    tar xzvf apache-maven-$MVN_VERSION-bin.tar.gz &&\
    rm apache-maven-$MVN_VERSION-bin.tar.gz &&\
    mv apache-maven-$MVN_VERSION $M2_HOME/ &&\
    \
    wget https://dl.bintray.com/sbt/native-packages/sbt/$SBT_VERSION/sbt-$SBT_VERSION.tgz &&\
    tar -xzvf sbt-$SBT_VERSION.tgz &&\
    mv sbt $SBT_HOME &&\
    rm sbt-$SBT_VERSION.tgz &&\
    \
    wget https://github.com/TresAmigosSD/spark/releases/download/1.5.2_hd/spark-1.5.2-bin-2.7.2.tgz &&\
    tar xzvf spark-1.5.2-bin-2.7.2.tgz &&\
    mv spark-1.5.2-bin-2.7.2 $SPARK_HOME/ &&\
    rm spark-1.5.2-bin-2.7.2.tgz &&\
    \
    useradd -G sudo --create-home --shell /bin/bash smv &&\
    echo "smv ALL = NOPASSWD: ALL" >> /etc/sudoers &&\
    mkdir /projects &&\
    chown smv:smv /projects &&\
    \
    #clean up
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/* &&\
    rm -rf /tmp/* /var/tmp/*

# Customizes prompt
COPY ./.bashrc /home/smv

USER smv
WORKDIR /projects

#---------------- above was smv-env image.
#FROM tresamigos/smv-env:env_jdk7_py27

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
