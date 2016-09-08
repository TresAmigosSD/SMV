FROM java:openjdk-7-jdk

ARG LIB_DIR=/usr/lib

ENV M2_HOME=$LIB_DIR/mvn \
    SPARK_HOME=$LIB_DIR/spark \
    SPARK_VERSION=1.5.2 \
    HADOOP_VERSION=2.6 \
    SBT_HOME=$LIB_DIR/sbt \
    SBT_VERSION=0.13.11 \
    MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m" \
    MVN_VERSION=3.3.9 \
    EDITOR=vim

ENV PATH=$PATH:$M2_HOME/bin:$SPARK_HOME/bin:$SBT_HOME/bin
    
ARG DEBIAN_FRONTEND=noninteractive

# Tools not needed for installation (e.g. git) should be added in the last RUN
# to avoid busting the cache and having to build Spark again
RUN echo "deb http://cran.rstudio.com/bin/linux/debian lenny-cran/" >> /etc/apt/source.list &&\
    apt-key adv --keyserver hkp://pgp.mit.edu --recv-key 381BA480 &&\
    apt-get update &&\
    apt-get install -y r-base &&\
    apt-get install -y unzip &&\
    apt-get install -y sudo &&\
    useradd -G sudo --create-home --shell /bin/bash smv &&\
    echo "smv ALL = NOPASSWD: ALL" >> /etc/sudoers

USER smv
WORKDIR /projects

RUN sudo mkdir -p $LIB_DIR &&\
    sudo chown smv:smv /projects &&\
    \
    wget http://www-us.apache.org/dist/maven/maven-3/$MVN_VERSION/binaries/apache-maven-$MVN_VERSION-bin.tar.gz &&\
    tar xzvf apache-maven-$MVN_VERSION-bin.tar.gz &&\
    rm apache-maven-$MVN_VERSION-bin.tar.gz &&\
    sudo mv apache-maven-$MVN_VERSION $M2_HOME/ &&\
    \
    wget https://dl.bintray.com/sbt/native-packages/sbt/$SBT_VERSION/sbt-$SBT_VERSION.tgz &&\
    tar -xzvf sbt-$SBT_VERSION.tgz &&\
    sudo mv sbt $SBT_HOME &&\
    rm sbt-$SBT_VERSION.tgz &&\
    # dry run of sbt forces download of sbt dependencies
    sbt exit &&\
    \
    wget https://github.com/Kai-Chen/spark/archive/1.5.2_hd.zip &&\
    unzip 1.5.2_hd.zip &&\
    sudo mv spark-1.5.2_hd $SPARK_HOME/ &&\
    rm 1.5.2_hd.zip &&\
    cd $SPARK_HOME &&\
    mvn -DskipTests -Psparkr -Phive -Phive-thriftserver -Pyarn -Phadoop-2.6 -Dhadoop.version=2.6.0 package


RUN sudo apt-get install -y $EDITOR &&\
    sudo apt-get install -y rsync &&\
    sudo apt-get install -y git

# Customizes prompt
COPY ./.bashrc /home/smv
