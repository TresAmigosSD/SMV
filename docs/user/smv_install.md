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

SMV works best with Python >= v2.7.x.  To verify the current version of python, run the following:

```bash
$ python --version
```

The correct version of python should already be present on Mac OSX.  For Ubuntu systems, python can be installed with:

```bash
$ sudo apt-get install python
```

# Install SMV

SMV installing requires either pip, curl, or wget to be available.  **Note:** The instructions below will also locally install the appropriate version of Apache Spark.  If Spark is already installed, then the `-spark` flag should be removed to skip the Spark installation.  If the Spark installation is skipped, then the current existing version of Spark **MUST** have been built with Hive support.

## Install SMV using pip

```bash
pip install smv  # install smv, assumes machine already has spark-submit + pyspark available
pip install smv[pyspark]  # installs pyspark along with smv
```

This will automatically add the SMV CLI scripts to the PATH inside of your Python environment

## Install SMV using curl

```bash
$ curl https://raw.githubusercontent.com/TresAmigosSD/SMV/v2r10/tools/smv-install | bash -s -- -spark
```

## Install SMV using wget

```bash
$ wget -qO- https://raw.githubusercontent.com/TresAmigosSD/SMV/v2r10/tools/smv-install | bash -s -- -w -spark
```

If curl/wget method doesn't work, we can use "sbt assembly" to build SMV by ourselves.

## Updating paths

The install script above will automatically update the user `.profile`, `.bash_profile`, or `.bashrc` files.  For the paths to take effect, user should log out, then log back in to update the paths in the shell.
Users can specify `--no-profile` to stop the update of the profile files.

## Verify SMV installation

To verify that SMV was installed correctly, run the following command:

```bash
$ type smv-run
```
The above should point to the `smv-run` script in the SMV tools directory.  If it can not find `smv-run` ensure that the profile rc file was sourced by either logging out / logging in or sourcing the appropriate profile file.

# Python packages

SMV depends on some third party python packages (e.g. jupyter, graphviz, etc).  To install these packages, use pip as follows:

```bash
$ dirname $(dirname $(type -p smv-run))  # this will show the SMV home directory.
$ pip install -r _SMV_HOME_/docker/smv/requirements.txt
```
