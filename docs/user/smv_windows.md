# SMV Windows Installation

**SMV DOES NOT SUPPORT WINDOWS, THIS DOC IS HISTORICAL AND JUST FOR REFERENCE**

SMV is currently supported on Linux (ubuntu, centos, etc) and OSX. Since Spark has Windows support, SMV can also
run on Windows. Currently SMV doesn't have scripts which help launching on Windows yet.

# Install JDK
Java 8 for Windows 10 should be able to download [here](https://www.java.com/en/download/win10.jsp).
After install, Java should have environment setup by itself.

# Install Python
SMV works best with python v2.7.x.
One can go [here](https://www.python.org/downloads/) to download v2.7.x. Install Python. Please
make sure you checked `pip`, when install Python.
Assume Python is installed on `C:\Python27`.

SMV can work with Jupyter Notebook. Since Jupyter Notebook provides a web interface, the user
experience will be the same on different systems (Windows, Linux or OSX), we highly recomment
to install Jupyter.

There are many ways to install Jupyter. Here is the `pip` way,
* Start a CMD as administrator
* `>pip install jupyter`

The first try might fail due to missing C++ compiler. Follow the pip message install C++ compiler,
and rerun `pip install jupyter` will solve the problem.

# Install Spark
SMV requires an Apache Spark version built with Hive support.  A prebuilt spark 1.5.2 version with Hive support can be found [here](https://github.com/TresAmigosSD/spark/releases/tag/1.5.2_hd).  Download the `spark-1.5.2-bin-2.7.2.tgz` bundle and extract it.
One may need a tool to extract `tgz` file. Suggest to use `7zip`.
Assume spark is extract to `%UserProfile%\spark-1.5.2-bin-2.7.2`.

# Install Winutils
Winutils is a package of Windows Binaries for Hadoop versions.
You can download it from [here](https://github.com/steveloughran/winutils). Please click `Clone or download` button on the
top-right, and then `Download Zip`. After download, extract the package. We only need the `hadoop-2.6.0` folder. Please copy
that folder to a safe place, say `%UserProfile%\hadoop-2.6.0`.

# Install SMV

A prebuilt SMV version can be found [here](https://github.com/TresAmigosSD/SMV/releases/tag/v1.5.2.2). Download the `smv_1.5.2.2.tgz` file and extract.
Assume smv is extract to `%UserProfile%\SMV_1.5.2.2`.

# Set environment variables
Start Windows control-panel and look for "environment variables" (you can search in Cortana. Add the following,

Variable | Value
--- | ---
HADOOP_HOME | `%UserProfile%\hadoop-2.6.0`
SPARK_HOME | `%UserProfile%\spark-1.5.2-bin-2.7.2`
PYTHONPATH | `C:\Python27`
SMV_HOME | `%UserProfile%\SMV_1.5.2.2`
\_JAVA_OPTION | `-Xmx512M -Xms512M`

Also need to add `%SPARK_HOME%\bin` and `%SMV_HOME%\tools` to the `Path` variable.

# Set Folder Permission
The HiveContext need to access the following 2 folders:
* `\tmp\hive`
* `%UserProfile%\AppData\Local\TEMP`

Need to set the Permission of those 2 folder to be `777`. Need to use the `winutils` to
do this.
* Open a CMD **as administrator**
* `C:\WINDOWS\system32>mkdir \tmp\hive`
* `C:\WINDOWS\system32>%HADOOP_HOME%\bin\winutils chmod 777 \tmp\hive`
* `C:\WINDOWS\system32>%HADOOP_HOME%\bin\winutils chmod 777 %UserProfile%\AppData\Local\TEMP`

# Verify Installation
Start a CMD.

Verify Python
```
>python --version
```

Verify Spark
```
>pyspark
...
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 1.5.2+hotdeploy
      /_/

Using Python version 2.7.13 (v2.7.13:a06454b1afa1, Dec 17 2016 20:42:59)
SparkContext available as sc, HiveContext available as sqlContext.
>>>sqlContext._ssql_ctx
```

# Create a Simplest SMV Pyshell Init file
Goto the SMV_HOME diretory, and create a file `smv_pyshell_init.py` with the following
content,
```python
# Get a random port for callback server
import random;
callback_server_port = random.randint(20000, 65535)

# Import commonly used pyspark lib
import pyspark.sql.functions
import pyspark.sql.types

sc.setLogLevel("ERROR")

# Imnport smv
from smv import smvPy
app = smvPy.init(['-m', 'None'], sc, sqlContext)

from smvshell import *
```

# Run SMV Pyshell

```
>set PYTHONPATH=%PYTHONPATH%;%SMV_HOME%\python
>set PYTHONSTARTUP=%SMV_HOME%\smv_pyshell_init.py
>pyspark --jars %SMV_HOME%\target\scala-2.10\smv-1.5-SNAPSHOT-jar-with-dependencies.jar --driver-class-path %SMV_HOME%\target\scala-2.10\smv-1.5-SNAPSHOT-jar-with-dependencies.jar
```

If you installed Jupyter
```
>set PYTHONPATH=%PYTHONPATH%;%SMV_HOME%\python
>set PYSPARK_DRIVER_PYTHON=\Python27\Scripts\jupyter.exe
>set PYSPARK_DRIVER_PYTHON_OPTS="notebook"
>set PYTHONSTARTUP=%SMV_HOME%\smv_pyshell_init.py
>pyspark --jars %SMV_HOME%\target\scala-2.10\smv-1.5-SNAPSHOT-jar-with-dependencies.jar --driver-class-path %SMV_HOME%\target\scala-2.10\smv-1.5-SNAPSHOT-jar-with-dependencies.jar
```
