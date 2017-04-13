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

# Install SMV
SMV installing requires either curl or wget to be available.  **Note:** The instructions below will also locally install the appropriate version of Apache Spark.  If Spark is already installed, then the `-spark` flag should be removed to skip the Spark installation.  If the Spark installation is skipped, then the current existing version of Spark **MUST** have been built with Hive support.

## Install SMV using curl
```bash
$ curl https://raw.githubusercontent.com/TresAmigosSD/SMV/master/tools/smv-install | bash -s -- -spark 1.5.2.5
```

## Install SMV using wget
```bash
$ wget -qO- https://raw.githubusercontent.com/TresAmigosSD/SMV/master/tools/smv-install | bash -s -- -spark 1.5.2.5
```

## Updating paths.
The install script above will automatically update the user `.profile`, `.bash_profile`, or `.bashrc` files.  For the paths to take effect, user should log out, then log back in to update the paths in the shell.

## Verify SMV installation
To verify that SMV was installed correctly, run the following command:
```shell
$ type smv-pyrun
```
The above should point to the `smv-pyrun` script in the SMV tools directory.  If it can not find `smv-pyrun` ensure that the profile rc file was sourced by either logging out / logging in or sourcing the appropriate profile file.
