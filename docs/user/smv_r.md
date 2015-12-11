# Run SMV App using Spark R Shell

### Synopsis
```shell
$ _SMV_HOME_/tools/smv-r [smv-options]
```

**Note:**  The above command should be run from the project top level directory.

<br>
<table>

<tr>
<th colspan="3">Options</th>
</tr>

<tr>
<th>Option</th>
<th>Default</th>
<th>Description</th>
</tr>

<tr>
<td>-g|-rs</td>
<td>false</td>
<td>Use graphical R shell (Rconsole or Rstudio). Not Supported Yet!!!</td>
</tr>

</table>


### Running SMV modules from R Shell

### runSmvModuleRdd

The `runSmvModuleRdd` method can be used to run a specific module.  It accepts a single string argument that specifies the name of the module to run.  The name can be specified in the same manner as the "-m" option to `smv-shell`.  It will return the Spark DataFrame result of running the module.  This result can be used with any of the standard SparkR commands.
For example:

```shell
$ smv-r
...
> df <- runSmvModuleRdd("EmploymentByState")
...
> count(df) // or other SparkR command
```

### runSmvModule

`runSmvModule` is a convenience function similar to `runSmvModuleRdd` but returns the local R DataFrame as the result.  This is the equivalent of calling `collect()` on the result from `smvRunModuleRdd`.

