# SMV Installation

SMV is currently supported on Linux (ubuntu, centos, etc) and OSX.

# Install JDK
SMV requires Sun Java JDK version 1.7 or 1.8.
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

# Install Python
SMV works best with python v2.7.x.  To verify the current version of python, run the following:
```
$ python --version
```

The correct version of python should already be present on Mac OSX.  For Ubuntu systems, python can be installed with:
```
$ sudo apt-get install python
```

# Install Spark
SMV requires an Apache Spark version built with Hive support.  A prebuilt spark 1.5.2 version with Hive support can be found [here](https://github.com/TresAmigosSD/spark/releases/tag/1.5.2_hd).  Download the `spark-1.5.2-bin-2.7.2.tgz` bundle and extract it.
```
$ tar xvf spark-1.5.2-bin-2.7.2.tgz
```

You must add the spark bin directory to your user `PATH` environment variable.  This is best done in `.profile` or `.bashrc` file.
```shell
export SPARK_HOME=Spark-install-dir
export PATH=$PATH:$SPARK_HOME/bin
```

To verify that Spark was installed correctly, run the following command:
```shell
$ spark-submit --version
```

# Install SMV

A prebuilt SMV version can be found [here](https://github.com/TresAmigosSD/SMV/releases/tag/v1.5.2.2). Download the `smv_1.5.2.2.tgz` file and extract as follows:
```
$ tar xvf smv_1.5.2.2.tgz
```

You must add the SMV bin directory to your user `PATH` environment variable.  This is best done in `.profile` or `.bashrc` file.
```shell
export SMV_HOME=SMV-install-dir
export PATH=$PATH:$SMV_HOME/tools
```

To verify that SMV was installed correctly, run the following command:
```shell
$ type smv-pyrun
```
The above should point to the `smv-pyrun` script in the SMV tools directory
