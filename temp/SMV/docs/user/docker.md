# Docker

Docker is a tool that creates lightweight containers for applications and services. One of the advantages of distributing an application with Docker is that once the user has Docker installed, they don't have to worry about _any_ other software dependencies - we just make sure that the our Docker image has the dependencies installed. That is why we recommend that users use SMV through Docker. This guide will walk you through the basics.

## Installation

You can find a guide to install Docker on your system [here](https://www.docker.com/products/overview#/install_the_platform).

## Starting an SMV container

With Docker installed, you can start an SMV container with
```
$ docker run -it --rm tresamigos/smv
```
All SMV tools will be in the container's path. Note that if you have not downloaded the Docker image of SMV before, Docker will download it for you automagically.  **Warning:** First download may need to download upward of 1 GB of data.  This may take a while on slow networks.

## Mounting a local directory

When developing an SMV application, you will want the project to live outside of the container you build and run it in. Just mount your projects directory into the container when you start it:
```
$ docker run -it --rm -v /path/to/myprojects:/projects tresamigos/smv
```
This will also enable you to edit your project in your favorite GUI editor.  **Note:** You should mount the directory where you store your projects and not a specific project.  The parent directory will also be used to cache repository data to avoid needless downloads.  For example:

```
$ mkdir ~/MyProjects
$ docker run -it --rm -v ~/MyProjects:/projects tresamigos/smv
```
Above command will put us in the docker environment (note change in prompt)
```
user@smv:/projects$ smv-init -s SampleProj
```
the above will create SampleProj under /projects directory in docker which is mounted to user `~/MyProjects`.
```
user@smv:/projects$ exit // exit docker environment
```
Once we exit docker environment, we are back to our normal host environment.  Note change back in prompt.
```
$ ls ~/MyProjects
SampleProj
```
Note that SampleProj now exists in host ~/MyProjects dir.

## Changing container user id.
On linux systems, if the host user id is not 1000 then the user may run into permission issues writing to the mounted directory (see above section).  For example, if the host user id is 501 and the mounted directory is owned by host user 501, then the container smv user (id 1000 by default) will not be able to write to the mounted directory unless the directory write permission is world writable (not likely).
To get around the issue, the user is able to supply the user id of the smv user in the container using the `-u` flag.
```
$ docker run -it --rm -v ~/MyProjects:/projects tresamigos/smv -u 501
```
Or to make it work with whatever the user id is:
```
$ docker run -it --rm -v ~/MyProjects:/projects tresamigos/smv -u $(id -u)
```
Note that `$(id -u)` returns the current user id on the host.
With the container smv user id set to be the same as the user id on the host, the smv user inside the container should be able to write to the mounted directory.

## Updating your SMV image

If you want to update to the most recent Docker image of SMV, use
```
$ docker pull tresamigos/smv
```
