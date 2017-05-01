# Module metadata

There are various contexts in which it would be useful to save metadata associated with modules, for example to have a record of the module's validation results. See #121, #630 for other examples. We currently save the schema and user-defined metadata associated with columns of a module in the `.schema` output directory of the module. A more general solution is needed to organize arbitrary metadata, some of which may be standard to SMV and some of which may be user-defined.

# .meta
 #332 and #121 propose using Json to structure the data in file, and Spark already a provides a generic `Metadata` class which implements conversion to (and, if necessary, from) Json. We will collect the metadata into a `Metadata` object and convert it to Json, then save it to file as an RDD (similar to what is currently done with the schema). The metadata will be stored with the same name as the `.csv` and `.schema` output is now, with a `.meta` extension.

# SmvMetadata

An extensible approach is needed for collecting metadata. We may need to add metadata as it becomes available (e.g. validation metadata will not be available until after the module is persisted). We will create an `SmvMetadata` class which accumulates metadata using Spark's `MetadataBuilder` and saves it to file when the module is persisted. In most contexts, we will be adding specific data with a known structure (e.g. schema data with optional metadata), and `SmvMetadata` will provide convenience methods that extract and organize the data (e.g. extracting the schema and metadata from the `DataFrame`). Eventually we can also enable users to add on their own arbitrary metadata, possibly by creating a Spark `Metadata` object.

# Configuration

We will create new SMV properties to configure what metadata to save. All metadata will be turned off with `smv.meta = false`. Additional properties (e.g. `smv.meta.edd`) will also be provided to control metadata with more granularity.

# Reading back metadata

There is currently no foreseen use for reading back metadata programatically. `smvserver` will need to serve metadata it can do so simply by sending the Json itself. For that reason, this design does not account for reading back metadata. However, should the need arise it would not be difficult to add such functionality, using Spark `Metadata's` `fromJson`.
