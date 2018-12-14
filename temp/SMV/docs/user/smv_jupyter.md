# Jupyter

[Jupyter](http://jupyter.org/) is an open source browser interface for running data science scripts interactively.

# SMV with Jupyter

Jupyter started out as iPython, an interactive python interpreter, and the initial installation of
Jupyter supports only Python code - even to use pyspark (without SMV) with Jupyter requires a special
configuration. `smv-jupyter` currently configures Jupyter with an
smv-pyshell [kernel](http://jupyter.readthedocs.io/en/latest/projects/kernels.html).

SMV currently tested with Jupyter with Python 2.7. AS long as Jupyter is installed and `jupyter`
command is in the `PATH`, `smv-jupyter` should be able to start the Jupyter server and can be
connected from a browser.


# How to use

One can install Jupyter himself on the machine he runs SMV, or use the smv docker image, which has
Jupyter installed.

Start SMV docker container with Jupyter port prepared,

```shell
$ docker run -it -p 8888:8888 -v /path/contains/proj:/projects tresamigos/smv
```

You can view your notebooks from the host at localhost:8888. If the docker command complains
that the port `8888` is used, you can map other host port to the docker container, e.g. `8889:8888`.

Either within the docker container or on a machine with SMV and Jupyter installed,
to run `smv-jupyter`, **you need to change
directory to the project root, then run `smv-jupyter`.
It will look for `nodebooks` directory under the project root for store notebooks.**

To use other directories to store the notebooks, one may need to create his own script to launch
Jupyter by using `tools/smv-jupyter` script as an example. Another way to do so is to use an user
conf file for Jupyter. For example,

```python
#In $HOME/.jupyter/jupyter_notebook_config.py
c.FileContentsManager.root_dir = u'othernotebooks'
c.NotebookApp.open_browser = False
```

With it `smv-jupyter` will use `othernotebooks` under the project root to store the notebooks.

# Work on a remote server

To take advantage of the power of Spark and SMV, we typically need to work on a remote server.
Basically, user need to connect to a server machine through SSH, and do development and data
investigation on the server. Assume user has a way to edit the project code on the server, here we
describe how one can use Jupyter on the remote server setup.

When run `smv-jupyter` on the server, it will open up a high port (e.g. 8888) if the port is available,
if not it will try some higher numbers. Please note that we use Jupyter in single user mode, in other
words, different team members should launch their own Jupyter server (using `smv-jupyter` command).

As Jupyter server started on the server, user can use SSH tunneling to map the remote server's Jupyter
port to local laptop. E.g. from laptop run the following command,
```
ssh -L 9000:localhost:8888 myusername@myserver.com
```
where `8888` should be replaced by the real port the Jupyter server is listening.

Then user can open a browser and type in
```
http://localhost:9000
```

For the first time connection, the user may ask to type in a token, which can be copy-pasted from
the Jupter server's starting shell.

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

Jupyter will load the `smv-pyshell`, so all the commands supported in the `smv-pyshell` will
be working in Jupyter.

In addition, for people who familiar with the `pandas` package, Jupyter provides additional
tools to `smv-pyshell`.

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
