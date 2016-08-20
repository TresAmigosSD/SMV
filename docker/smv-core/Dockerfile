FROM java:openjdk-7-jdk

ARG LIB_DIR=/usr/lib

ENV M2_HOME=$LIB_DIR/mvn \
    SPARK_HOME=$LIB_DIR/spark \
    SPARK_VERSION=1.5.2 \
    HADOOP_VERSION=2.6 \
    SBT_HOME=$LIB_DIR/sbt \
    SBT_VERSION=0.13.11 \
    MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m" \
    MVN_VERSION=3.3.9 \
    EDITOR=vim

ENV PATH=$PATH:$M2_HOME/bin:$SPARK_HOME/bin:$SBT_HOME/bin
    
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update &&\
    apt-get install -y $EDITOR &&\
    apt-get install -y unzip &&\
    apt-get install -y rsync &&\
    apt-get install -y sudo &&\
    apt-get install -y git &&\
    useradd -G sudo --create-home --shell /bin/bash smv &&\
    echo "smv ALL = NOPASSWD: ALL" >> /etc/sudoers

USER smv
WORKDIR /projects

RUN sudo mkdir -p $LIB_DIR &&\
    sudo chown smv:smv /projects &&\
    wget http://www-us.apache.org/dist/maven/maven-3/$MVN_VERSION/binaries/apache-maven-$MVN_VERSION-bin.tar.gz &&\
    tar xzvf apache-maven-$MVN_VERSION-bin.tar.gz &&\
    rm apache-maven-$MVN_VERSION-bin.tar.gz &&\
    sudo mv apache-maven-$MVN_VERSION $M2_HOME/ &&\
    wget http://d3kbcqa49mib13.cloudfront.net/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz &&\
    tar xzvf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz &&\
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz &&\
    sudo mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME/ &&\
    wget https://dl.bintray.com/sbt/native-packages/sbt/$SBT_VERSION/sbt-$SBT_VERSION.tgz &&\
    tar -xzvf sbt-$SBT_VERSION.tgz &&\
    sudo mv sbt $SBT_HOME &&\
    rm sbt-$SBT_VERSION.tgz &&\
    sbt exit

#Custom .bashrc customizes prompt
COPY ./.bashrc /home/smv