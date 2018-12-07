# SMV Output Modules

This document is for output module classes defined in `smv.iomod`. The mixin `SmvOutput` in `smv` package is deprecated.

SMV output modules write data through some **Connections** to a table or file(s)

SMV support the following types of outputs:

* `SmvCsvOutputFile`: Comma Separated Values (CSV) file on HDFS compatible storage
* `SmvHiveOutputTable`: Hive tables (as long as Spark can access the H-catalog)
* `SmvJdbcOutputTable`: JDBC tables (any DB which supports JDBC connector, many need to load specific JDBC jar for the given DB)

Each output module depends on a single SmvGenericData, and the only thing the 
output module does is to write.

## Examples

### SmvCsvOutputFile

```Python
class CsvOut(SmvCsvOutputFile):
    def connectionName(self):
        return "my_hdfs"

    def fileName(self):
        return "csv_out.csv"

    def requiresDS(self):
        return [MyData]
```

With the following conf:
```
smv.conn.my_hdfs.type = hdfs
smv.conn.my_hdfs.path = /path/to/outputdata
```

### SmvHiveOutputTable

```python
class NewHiveOutput(SmvHiveOutputTable):
    def requiresDS(self):
        return [NewHiveInput]
    def tableName(self): return "WriteOutM"
    def connectionName(self): return "my_hive"
```

With the following conf:

```
smv.conn.my_hive.type = hive
smv.conn.my_hive.schema = mydbschema
```

### SmvJdbcOutputTable

User need to make the correct JDBC driver available in the classpath. For example, for PostgreSql, need to have the following be part of launching script.

```
... --driver-class-path /path/to/postgresql-42.1.4.jar --jars /path/to/postgresql-42.1.4.jar ...
```

```Python
class NewJdbcOutputTable(SmvJdbcOutputTable):
    def requiresDS(self):
        return [MyJdbcModule]

    def tableName(self):
        return "MyJdbcTable"

    def writeMode(self):
        return "overwrite"

    def connectionName(self):
        return "my_jdbc"
```

With conf:
```
smv.conn.my_jdbc.type = jdbc
smv.conn.my_jdbc.url = jdbc:postgresql://localhost/test
smv.conn.myjdbc_conn.driver= org.postgresql.Driver
```
