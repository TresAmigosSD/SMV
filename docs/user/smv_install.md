# SMV Installation

SMV is currently supported on Linux (ubuntu, centos, etc) and OSX.

# Install JDK
SMV requires Sun Java JDK version 1.7 or 1.8.  This has not been tested with OpenJDK.
Download Oracle JDK from [JDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html)

For Ubuntu, please make sure to set the Sun java jdk as the default (See [Install JDK on Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-java-on-ubuntu-with-apt-get) for details)

* Ensure JAVA_HOME environment variable is set in user profile and added to PATH:
```shell
$ export JAVA_HOME=jdk-install-dir
$ export PATH=$PATH:$JAVA_HOME/bin
```
* Verify JDK installlation in a new shell:
```shell
$ echo $JAVA_HOME
$ java -version
```

# Install Maven
Building Spark using Maven requires Maven 3.3.3 or newer. Follow the [Maven installation instructions](http://maven.apache.org/install.html)
* Adding to PATH:
```shell
$ export PATH=$PATH:maven-install-dir/bin
```
* Verify Maven installlation in a new shell:
```shell
$ mvn -v
```
Learn more about [how to use mvn command](https://maven.apache.org/run.html)

# Install Spark
Download the 1.5.2 version of [Spark](http://spark.apache.org/downloads.html).
There are two options to install Spark:

* "Happy Option": Download the Spark prebuild binary

For normal users who don't need to debug, it is recommended to download the Spark prebuild binary. 
Please use version build for Hadoop 2.4 or 2.6 and later (Please do NOT use the version without hadoop as hadoop support is required by SMV). You just need to download, decompress, and finish the environment setting and verify steps below.

* "Thorough Option": Build Spark from source code

For developers who may need to debug, it is recommended to build Spark from source code.
[Scala 2.10.4](http://www.scala-lang.org/download/2.10.4.html) is needed to build Spark 1.5.2.

Please do NOT use scala version 2.11+, because Spark does not yet support its JDBC component for Scala 2.11, while Spark hive is needed by SMV.

After Scala is downloaded and decompress, add it to PATH and verify Scala installation with
```shell
$ scala -version
```

After the Spark source was downloaded, run the following command under Spark directory:
```shell
$ mvn clean install -Phive -Phive-thriftserver -DskipTests 
```

Note: Spark hive is needed in SMV and to enable Hive integration for Spark SQL along with its JDBC server and CLI, `-Phive and Phive-thriftserver` are needed as build options. Sometimes SPARK source code test the Hadoop environment so install on standalone machine without Hadoop may cause test failure. So we suggest to add `-DskipTests` as build option.

For more information regarding building Spark, please refer to the [official document](http://spark.apache.org/docs/1.5.2/building-spark.html)


* Spark environment setting in user profile:
```shell
$ export SPARK_HOME=Spark-install-dir
$ export PATH=$PATH:$SPARK_HOME/bin
```

* To verify that Spark was installed correctly, run the following command:
```shell
$ spark-submit --version
```

# Install SMV

1. Download [the latest SMV source code](https://github.com/TresAmigosSD/SMV/archive/master.zip)

2. Build the package.
```shell
$ mvn clean install
```

In case of using JDK7, you may need to set MAVEN_OPTS to avoid out of memory issue
```shell
export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m"
```
