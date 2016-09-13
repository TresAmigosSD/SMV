FROM java:openjdk-7-jdk

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
    # dry run of sbt forces download of sbt dependencies
    sbt exit &&\
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