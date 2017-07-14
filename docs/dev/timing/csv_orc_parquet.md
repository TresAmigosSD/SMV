Historically, SMV has directly supported writing data as CSV only. Spark also supports Apache data formats ORC and Parquet, both of which are columnar and compressed. There are several reasons to examine these formats as alternatives to CSV for persisting intermediate results, including reduced disk space usage and wall-clock speed ups for systems with slow file IO (such as clusters which access the disk over a network). The following measurements were taken (on a 2016 MBP with SSD and 16g memory) to get a general comparison of IO speeds. All operations are on same data set generated with tpch-dbgen, which was 7.7g in CSV format, 3.1g in ORC, and 3.2g in Parquet.


# Time to write CSV
192.41

# Time to write ORC
206.41s

# Time to write Parquet
203.61s

**note**: the above measurements were taken writing with the Spark DataFrameWriter

# Time to read CSV with a filter
.041s

# Time to read ORC with a filter
.088s

# Time to read Parquet with a filter
.111s

**note**: the filter filtered out about 3/4 of the data

# Time to read CSV with aggregation
93.17s

# Time to read ORC with aggregation
15.44s

# Time to read Parquet with aggregation
8.18s
