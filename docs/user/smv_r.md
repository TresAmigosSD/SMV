# Run SMV App using Spark R Shell

## Synopsis
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
<td>Use graphical R shell (Rconsole or Rstudio). Only supported in Mac OS, for Linux need to modify the smv-r script slightly</td>
</tr>

</table>

## Use SparkR and smvR in R

`smv-r` shell script simply setup some environment variables and start R in shell or Rstudio with
* library path configured to include `SparkR` and `smvR` packages
* preload `sc` for SparkContext, `sqlContext` and `smvApp` in the `.GlobalEnv`

As a contrary to the `sparkR` command provided by Spark itself, `smv-r` does NOT load either `SparkR` or `smvR` as `defaultPackages`.
To use functions in them, user should either attach them:
```r
library(SparkR)
library(smvR)
```  
or refer the functions with the package name:
```r
SparkR::select(...)
smvR::runSmvModuleRdd(...)
```

The reason that we are not loading `SparkR` package as default is that some base functions, such as `table`, are masked ("overload") by `SparkR` package,
which could be very confusing. We'd rather let user to attach `SparkR` explicitly.

In the rest of the document, we assume `SparkR` and `smvR` are attached.    

## Running SMV modules from R Shell

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

### dynSmvModuleRdd

The `dynSmvModuleRdd` method is similar to `runSmvModuleRdd`, it can run a specified
module. The difference is that this method load the module dynamically. Please refer
[Dynamic Class Loading](class_loader.md) for the concept of dynamic module loading.
Currently this method only accept FQN as module name.
For example:

```shell
$ smv-r
...
> df <- dymSmvModuleRdd("com.mycompany.MyApp.stage2.StageEmpCategory")
...
> count(df) // or other SparkR command
```


### runSmvModule

`runSmvModule` is a convenience function similar to `runSmvModuleRdd` but returns the local R DataFrame as the result.  This is the equivalent of calling `collect()` on the result from `smvRunModuleRdd`.
