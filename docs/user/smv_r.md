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
<td>-g</td>
<td>false</td>
<td>Use graphical R shell (Rstudio). Not Supported Yet!!!</td>
</tr>

</table>


### Running SMV modules from R Shell

The `runSmvModule` method can be used to run a specific module.  It accepts a single string argument that specifies the name of the module to run.  The name can be specified in the same manner as the "-m" option to `smv-shell`.
For example:

```shell
$ smv-r
...
> df <- runSmvModule("EmploymentByState")
...
> head(df) // or other standard R commands.
```