# Create Schema for new Csv files

As in the example project created by the `smv-init` command (see [Getting Started](getting_started.md)
for details), there is a `Csv` data file, `data/input/employment/CB1200CZ11.csv`. Also there is
a `.schema` file associated with the `Csv` file.

However, when we just download the data from it's original source, there is no Schema file. So to make
data usable for SMV and Spark, we need to create the Schema file. SMV has a Schema discovery tool to
help user quickly create it.

## Where to put the Csv files

SMV will look for `smv.inputDir` when it load the configuration(see [Application Configuration](app_config.md) for deatails). If multiple places defined the parameter, the order of
priority from high to low is,

* command line options
* project user level config file (default: conf/smv-user-conf.props)
* global user level config file (~/.smv/smv-user-conf.props)
* app level config file (default: conf/smv-app-conf.props)  

If `smv.inputDir` is not specified, the default value will be a sub-dir of `smv.dataDir` with name `input`.
`smv.dataDir` must be specified for each Smv application.

Please note that in a cluster environment, the `smv.inputDir` and `smv.dataDir` might be on `HDSF`, hdfs command might be
needed to setup the directory structures.

## Discover Schema from Shell

Within the SMV Spark shell environment (see [Run Spark Shell](run_shell.md) for details), a
`smvDiscoverSchemaToFile` command is provided.

```python
> smvDiscoverSchemaToFile("/path/to/file.csv")
```

For above case, the Csv file is assumed to be
* Comma delimited
* with a single line header

You can specify the appropriate CsvAttributes for your file

```python
> smvDiscoverSchemaToFile("/path/to/file.csv", ca = CsvAttributes(delimiter = '|', hasHeader = True))
```

Please see [SMV File](smv_input.md) for more CsvAttributes details.

**Note** SMV currently can't handle Csv files with multiple lines of header. Other tools might be needed
to remove extra header lines before try to discover schema.

The shell `smvDiscoverSchemaToFile` method will create a schema file on the local running dir with name  `file.schema.toBeReviewed`. As
hinted by the file name, human need to review the schema file.

Using the `CB1200CZ11.csv` file as an example,

```python
>>> smvDiscoverSchemaToFile("data/input/employment/CB1200CZ11.csv", ca = CsvAttributes(delimiter = '|', hasHeader = true))
```

The path here is relative to the project root dir, where I started the `smv-shell`.

A `CB1200CZ11.schema.toBeReviewed` file is generated. The first a couple of lines are
```
@delimiter = ,
@has-header = true
@quote-char = "
ST: Integer
ZIPCODE: Integer
GEO_ID: String
GEO_TTL: String
...
```

As you can see that `ZIPCODE` is identified as `Integer`. In the review, one should edit the
file and change it to `String`, since we don't want to do any arithmetic on `ZIPCODE` and we don't
want to consider `04532` as `4532`.

After review and editing, you can rename the file by removing the postfix, and move it to the same dir
of the `csv` file. Now the data is ready for SMV.

**Note** field names with "." in  them may cause columns unable to be resolved, therefore, avoid field names that has "." in them".


For Csv data without header, the field name in the generated schema file will be `f1`, `f2`, etc.
