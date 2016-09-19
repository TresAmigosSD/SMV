# Jupyter

[Jupyter](http://jupyter.org/) is an open source browser interface for running data science scripts interactively. 

# SMV with Jupyter

The smv-jupyter docker image makes it easy to run SMV code through Jupyter. Jupyter started out as iPython, an interactive python interpreter, and the initial installation of Jupyter supports only Python code - even to use pyspark (without SMV) with Jupyter requires a special configuration. smv-jupyter currently configures Jupyter with an smv-pyshell [kernel](http://jupyter.readthedocs.io/en/latest/projects/kernels.html). Support for SMV in Scala and R is in the works.

# How to use

Build the docker image yourself with 

```shell
$ docker build -t smv-jupyter _SMV_HOME_/docker/smv-jupyter
```

and run on a compiled project with

```shell
$ docker run -it -p 8888:8888 -v /path/to/proj:/proj smv-jupyter
```

Any arguments following smv-jupyter will be passed to jupyter notebook

You can view your notebooks at localhost:8888.