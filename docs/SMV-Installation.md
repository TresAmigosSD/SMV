SMV Installation
===

Spark Model Variables

This page will illustrate how to install [SMV](https://github.com/TresAmigosSD/SMV) in 
a Ubuntu machine step by step.

This page described earlier version of SMV with Spark 1.1. The latest SMV snapshot 
actually works with Spark 1.3.  

Install JDK
===========
Spark should work on all kinds of JDK's. Here is only one way to set it up.

* Go to page of [JDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html) and agree the license agreement.
* Download proper source file like **jdk-7u75-linux-x64.tar.gz**.
* Unpack package with 
```shell
$ sudo tar -zxvf jdk-7u75-linux-x64.tar.gz –C /install/path
```
* Configure environment file via ```$ vim ~/.bashrc``` adding following lines
```shell
export JAVA_HOME=/install/path/jdk1.7.0_75
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export PATH=${JAVA_HOME}/bin:$PATH
```
Then source the enviroment file via ```$ source ~/.bashrc```
* Set as default JDK if there is already other versions like Open JDK
```shell
$ sudo update-alternatives --install /usr/bin/java java /install/path/jdk1.7.0_75/bin/java 300
$ sudo update-alternatives --install /usr/bin/javac javac /install/path/jdk1.7.0_75/bin/javac 300
$ sudo update-alternatives --install /usr/bin/jar jar /install/path/jdk1.7.0_75/bin/jar 300 
$ sudo update-alternatives --install /usr/bin/javah javah /install/path/jdk1.7.0_75/bin/javah 300 
$ sudo update-alternatives --install /usr/bin/javap javap /install/path/jdk1.7.0_75/bin/javap 300
```
Select after executing ```$ sudo update-alternatives --config java```

Installing Scala is not necessary as it is included in Spark 
========

Install Spark
=======

* Download [Spark](http://mirror.bit.edu.cn/apache/spark/spark-1.1.0/spark-1.1.0-bin-hadoop2.4.tgz) pre-built package of versio 1.1.0.
```shell
$ wget -c http://mirror.bit.edu.cn/apache/spark/spark-1.1.0/spark-1.1.0-bin-hadoop2.4.tgz
```
* Unpack package with 
```shell
$ sudo tar -zxvf spark-1.1.0-bin-hadoop2.4.tgz –C /install/path
```
* Configure environment file via ```vim ~/.bashrc``` adding following lines
```shell
export SPARK_HOME=/install/path/spark-1.1.0-bin-hadoop2.4
export PATH=${SPARK_HOME}/bin:$PATH
```
Then source the enviroment file via ```$ source ~/.bashrc```
* Sample test to estimate value of Pi
```shell
$ run-example SparkPi 2 local
```

Install Git and Maven
=======
* Install git with ```sudo apt-get install git```
* Install Maven
** Download latest version of [Maven](http://maven.apache.org/) via
```shell
wget -c http://mirror.bit.edu.cn/apache/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz
```
* Unpack package with 
```shell
$ sudo tar -zxvf apache-maven-3.2.5-bin.tar.gz –C /install/path
```
* Configure environment file via ```vim ~/.bashrc``` adding following lines
```shell
export MAVEN_HOME=/install/path/apache-maven-3.2.5
export PATH=${MAVEN_HOME}/bin:$PATH
```
Then source the enviroment file via ```$ source ~/.bashrc```
* Check the version via ```$ mvn -version```

Install SMV
=======
* Clone the source file.
```shell
$ git clone https://github.com/TresAmigosSD/SMV.git
```
* Build the package.
```shell
$ mvn clean install
```

SMV Test of EDD
=======
Download sample zip level employment data via
```shell
$ http://www2.census.gov/econ2012/CB/sector00/CB1200CZ11.zip 
$ unzip CB1200CZ11.zip
```

Pre-load SMV jar when run spark-shell. 

```shell
$ ADD_JARS=/path/to/smv/target/smv-1.0-SNAPSHOT.jar spark-shell -i sparkEDD.scala
```
where `sparkEDD.scala` will be loaded for convenience. It could look like the follows,

```scala
import org.apache.spark.sql.SQLContext
import org.tresamigos.smv._
val sqlContext = new SQLContext(sc)
import sqlContext._
import org.apache.spark.sql.catalyst.expressions._
implicit val ca=CsvAttributes(delimiter = '|', hasHeader = true)
val file=sqlContext.csvFileWithSchemaDiscovery("CB1200CZ11.dat", 100000)
val res = file.edd.addBaseTasks().createReport
res.saveAsTextFile("report/edd")
```
The output in `report/edd` should like
```
Total Record Count:                        38818
ST                   Non-Null Count:        38818
ST                   Average:               29.266989540934617
ST                   Standard Deviation:    15.489890281737008
ST                   Min:                   1
ST                   Max:                   99
ZIPCODE              Non-Null Count:        38818
ZIPCODE              Average:               49750.09928383732
ZIPCODE              Standard Deviation:    27804.662445936523
ZIPCODE              Min:                   501
```
