# SMV Installation

SMV is currently supported on Linux (ubuntu, centos, etc) and OSX.

# Install JDK
SMV requires Sun Java JDK version 1.7 or 1.8.  This has not been tested with OpenJDK.
Download Java JDK from [JDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html)

For Ubuntu, please make sure to set the Sun java jdk as the default (See [Install JDK on Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-java-on-ubuntu-with-apt-get) for details)

# Install Scala
SMV requires Scala version 2.10.4+. Download [Scala 2.10.4](http://www.scala-lang.org/download/2.10.4.html)

If you are going to build Spark from source later, please do NOT use scala version 2.11+. Because Spark does not yet support its JDBC component for Scala 2.11, while Spark hive is needed by SMV.

# Install Maven
Building Spark using Maven requires Maven 3.3.3 or newer. Follow the [Maven installation instructions](http://maven.apache.org/install.html)

# Install Spark
Download the 1.5.2 version of Spark from [Spark](http://spark.apache.org/downloads.html).
You can either build Spark from source or download the spark binary. It is recommended to build Spark from source, because the source is quite usefull for debugging.

* Option1: Build Spark from source

After the Spark source was downloaded, then it needs to build as follows:
```
$ cd SPARK_HOME; mvn clean install -Phive -Phive-thriftserver -DskipTests 
```

Spark hive is needed in SMV. To enable Hive integration for Spark SQL along with its JDBC server and CLI, `-Phive and Phive-thriftserver` are needed as build options. 

Sometimes SPARK source code test the Hadoop environment so install on standalone machine without Hadoop may cause test failure. So we suggeste to add `-DskipTests` as build option.

For more information regarding building Spark, please refer to the [official document](http://spark.apache.org/docs/1.5.2/building-spark.html)

* Option2: Download the Spark binary

If you still run into issues of install Spark from the source, you can try one of the binary
version. Any binary version should work on standalone machines. Suggest to use version build
for Hadoop 2.4 or 2.6 and later (Please do NOT use the version without hadoop as hadoop support
is required by SMV). You just need to download, decompress, and follow the next steps.

Add the Spark `bin` directory to the `PATH` environment variable. For example:
```shell
$ export PATH="${PATH}:SPARK_HOME/bin"
```
where `SPARK_HOME` is the actual directory where Spark was installed.

To verify that Spark was installed correctly, run the following command:
```shell
$ spark-submit -h
```

# Install Git

follow the [Git installation instructions](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)


# Install SMV

1. Clone the SMV source repository.

```shell
$ git clone https://github.com/TresAmigosSD/SMV.git
```

2. Build the package.

```shell
$ mvn clean install
```

You may need to set MAVEN_OPTS to avoid out of memory issue.
```shell
export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m"
```
