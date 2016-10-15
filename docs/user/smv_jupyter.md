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

You could also install Jupyter on your host machine without docker. To run `smv-jupyter`, you need to change
directory to the project root, compile the project with `mvn package` or `sbt assembly`, then run `smv-jupyter`.
It will look for `nodebooks` directory under the project root for store notebooks. 

# Jupyter notebook keyboard shortcuts

```
Command Mode (press Esc to enable)

Enter              enter edit mode
Shift-Enter        run cell, and select below
Ctrl-Enter         run cell
Alt-Enter          run cell, and insert below
M                  to markdown
1                  to heading 1
2,3,4,5,6          to heading 2,3,4,5,6
X                  cut selected cell
C                  copy selected cell
Z                  undo last cell deletion
Shift-M            merge cell below
```

```
Edit Mode (press Enter to enable)

Tab                code completion or indent
Ctrl-Up            go to cell start
Ctrl-Down          go to cell end
Esc                enter command mode
Shift-Enter        run cell, select below
```

For more shortcuts, refer [Jupyter notebook keyboard shortcuts](https://www.cheatography.com/weidadeyue/cheat-sheets/jupyter-notebook/).

# View data

View the first `n` rows of the dataframe `df` with

```
df.limit(n).toPandas().head(n)
```

The default maximal number of columns to display is 20, and maximal number of rows to display is 60. To display more rows or columns, set display options with

```
import pandas as pd
pd.set_option("display.max_columns", column_number)
pd.set_option("display.max_rows", row_number)
```

Set float number display precision with

```
pd.set_option('display.precision', 2)
```

# Data visualization

Install `matplotlib` for data visualization, and import with

```
import matplotlib.pyplot as plt
%matplotlib inline
```

X-Y plot  

```
df.plot(x='column1', y='column2' )
```

Bar plot

```
df['column1'].plot(kind='bar')
```

For more commands and options of plot, refer [matplotlib documentation](http://matplotlib.org/).

[Seaborn](https://github.com/mwaskom/seaborn) is a visualization library based on matplotlib for statistical plotting, and provides preset styles and color palettes for more attractive figures.
