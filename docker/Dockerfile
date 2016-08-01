FROM java:openjdk-7-jdk

ENV SMV_USER_LIB=/home/smv/lib \
    M2_HOME=/home/smv/lib/mvn \
    SPARK_HOME=/home/smv/lib/spark \
    SPARK_VERSION=1.5.2 \
    HADOOP_VERSION=2.6 \
    SBT_HOME=/home/smv/lib/sbt \
    SBT_VERSION=0.13.11 \
    SMV_HOME=/home/smv/lib/SMV \
    MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m" \
    MVN_VERSION=3.3.9 \
    EDITOR=vim
    
ARG DEBIAN_FRONTEND=noninteractive

COPY ./entrypoint.sh /usr/bin/entrypoint.sh

#apt-get must be run as root
RUN apt-get update &&\
    apt-get install -y $EDITOR &&\
    apt-get install -y unzip &&\
    apt-get install -y rsync &&\
    useradd --create-home --shell /bin/bash smv &&\
    mkdir /projects &&\
    chown smv:smv /projects

USER smv
WORKDIR /projects
ENV PATH=$PATH:$M2_HOME/bin:$SPARK_HOME/bin:$SBT_HOME/bin:$SMV_HOME/tools

# Install maven, sbt, spark, smv
RUN mkdir $SMV_USER_LIB &&\
    wget http://www-us.apache.org/dist/maven/maven-3/$MVN_VERSION/binaries/apache-maven-$MVN_VERSION-bin.tar.gz &&\
    tar xzvf apache-maven-$MVN_VERSION-bin.tar.gz &&\
    rm apache-maven-$MVN_VERSION-bin.tar.gz &&\
    mv apache-maven-$MVN_VERSION $M2_HOME/ &&\
    wget http://d3kbcqa49mib13.cloudfront.net/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz &&\
    tar xzvf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz &&\
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz &&\
    mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME/ &&\
    wget https://dl.bintray.com/sbt/native-packages/sbt/$SBT_VERSION/sbt-$SBT_VERSION.tgz &&\
    tar -xzvf sbt-$SBT_VERSION.tgz &&\
    mv sbt /home/smv/lib &&\
    rm sbt-$SBT_VERSION.tgz &&\
    wget http://github.com/TresAmigosSD/SMV/archive/master.zip &&\
    unzip master.zip &&\
    rm master.zip &&\
    mv SMV-master $SMV_HOME/ &&\
    cd $SMV_HOME &&\
    mvn clean install -DskipTests &&\
    sbt publish-local

#Custom mvn settings cofigure repository to live on user's host mount but also leverage cache from SMV build
COPY ./settings.xml $M2_HOME/conf/settings.xml

#Custom sbtops configures sbt to use ivy dependency and local caches on host mount
COPY ./sbtopts /home/smv/lib/sbt/conf/sbtopts

#Custom .bashrc customizes prompt
COPY ./.bashrc /home/smv


#Must use bracket syntax (["command"]) so that user can supply params
ENTRYPOINT ["entrypoint.sh"]