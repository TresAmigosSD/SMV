# Docker

Docker is a tool that creates lightweight containers for applications and services. One of the advantages of distributing an application with Docker is that once the user has Docker installed, they don't have to worry about _any_ other software dependencies - we just make sure that the our Docker image has the dependencies installed. That is why we recommend that users use SMV through Docker. This guide will walk you through the basics.

## Installation

You can find a guide to install Docker on your system [here](https://www.docker.com/products/overview#/install_the_platform).

## Starting an SMV container

With Docker installed, you can start an SMV container with
```
$ docker run -it --rm tresamigos/smv
```
All SMV tools will be in the container's path. Note that if you have not downloaded the Docker image of SMV before, Docker will download it for you automagically.

## Mounting a local directory

When developing an SMV application, you will want the project to live outside of the container you build and run it in. Just mount your project into the container when you start it:
```
$ docker run -it --rm -v /path/to/myproject:/projects/myproject tresamigos/smv
```
This will also enable you to edit your project in your favorite GUI editor.

## Updating your SMV image

If you want to update to the most recent Docker image of SMV, use
```
$ docker pull tresamigos/smv
```
