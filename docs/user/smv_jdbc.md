# SMV with JDBC

SMV supports both reading and writing data over a JDBC connection. Note that this has not been tested across all connection types and drivers.

## Configuration

Reading or writing over a JDBC connection requires
- a JDBC driver
- a JDBC url

### JDBC driver

You will need make the correct JDBC driver available in the classpath. JDBC drivers are specific to the type of database you want to connect to. Once you have identified the correct driver, you can include it in the class path with the Spark `--jars` option.
The driver needs to be specified with the SMV property `smv.jdbc.driver`

### JDBC url

You will need to provide the JDBC url needed to connect to the database. JDBC url format varies by connection type. Once you have identified the correct url, you can include it in the class path with SMV property `smv.jdbc.url`. This can be set in the app or user config, or alternatively at the commandline.

# Read data over JDBC with SmvJdbcTable

Data can be read over JDBC using `SmvJdbcTable`. Read more [here](smv_input.md#jdbc-inputs).

# Write data over JDBC with --publish-jdbc

When running an application, you may publish data over a JDBC connection using the `--publish-jdbc` option. Make sure that the application is properly configured when you do this. For example
```shell
$ smv-pyrun --run-app --publish-jdbc --smv-props smv.jdbc.url="my-url" smv.jdbc.driver="my.jdbc.driver.classname" -- --jars "my-jdbc-driver.jar"
```
