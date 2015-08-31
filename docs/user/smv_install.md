# SMV Installation

SMV is currently supported on Linux (ubuntu, centos, etc) and OSX.

# Install JDK
SMV requires Sun Java JDK version 1.7 or 1.8.  This has not been tested with OpenJDK.
Download Java JDK from [JDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html)

For Ubuntu, please make sure to set the Sun java jdk as the default (See [Install JDK on Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-java-on-ubuntu-with-apt-get) for details)

# Install Spark
Download the 1.3.0 version of Spark from [Spark](http://spark.apache.org/downloads.html).
It is recommended that the source version of spark is used rather than a binary download.
The source is quite usefull for debugging.

If the Spark source was downloaded, then it needs to build as follows:
```
$ cd SPARK_HOME; mvn install
```

# Install Git and Maven

* For git, follow the [Git installation instructions](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* For maven, follow the [Maven installation instructions](http://maven.apache.org/install.html)


# Install SMV

1. Clone the SMV source repository.

```shell
$ git clone https://github.com/TresAmigosSD/SMV.git
```

2. Build the package.

```shell
$ mvn clean install
```
