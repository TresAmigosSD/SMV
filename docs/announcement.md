# Support All Spark Command Line Parameters in SMV Commands 

Please see #415 for the motivation and design.
Please see 85decb76ba72a7d5e85e0d070440b1abf890f711 for the code change

Before the change we selectively supported some Spark command line parameters in the smv commands,
* smv-shell
* smv-run
* smv-pyshell
* smv-pyrun

With the change, we specified a parameter pattern so that the smv commands can figure out which parts
are smv parameters and which parts are Spark parameters.

Here is an example:
```shell
smv-run -m a.b.c -- --executor-memory=6G  --driver-memory=2G
```

Please refer the following user doc for the new command line syntax.

* [Run Application](run_app.md)
* [Run Spark Shell](run_shell.md)
