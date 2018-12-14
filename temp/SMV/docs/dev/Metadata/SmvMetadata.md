# Module metadata

There are various contexts in which it would be useful to save metadata associated with modules, for example to have a record of the module's validation results. See #121, #630 for other examples. We currently save the schema and user-defined metadata associated with columns of a module in the `.schema` output directory of the module. A more general solution is needed to organize arbitrary metadata, some of which may be standard to SMV and some of which may be user-defined.

# .meta
 #332 and #121 propose using Json to structure the data in file, and Spark already a provides a generic `Metadata` class which implements conversion to (and, if necessary, from) Json. We will collect the metadata into a `Metadata` object and convert it to Json, then save it to file. The metadata will be stored with the same name as the `.csv` and `.schema` output is now, with a `.meta` extension.

# SmvMetadata

 We will create an `SmvMetadata` class which accumulates metadata using Spark's `MetadataBuilder` and saves it to file when the module is persisted. In most contexts, we will be adding specific data with a known structure (e.g. schema data with optional metadata), and `SmvMetadata` will provide convenience methods that extract and organize certain kinds of data (e.g. extracting the schema and metadata from the `DataFrame`). Eventually we can also enable users to add on their own arbitrary metadata. This will collect knowledge about metadata into one place and also ensure encapsulate Spark's `Metadata`, which is part of Spark's DeveloperAPI and which may change with minor versions of Spark.

# Saving metadata

Metadata should be saved at the time that the module is run, but this cannot be part of `SmvModule's` persist operation because `SmvInputs` can also have metadata associated with them. Saving metadata will be part of `SmvDataSets'` `computeDataFrame` method. The Json string can be saved to file as a single line RDD, similar to what's done today with the schema.

# Configuration

We will create new SMV properties to configure what metadata to save. All metadata will be turned off with `smv.meta = false`. Additional properties (e.g. `smv.meta.edd`) will also be provided to control metadata with more granularity.

# Retrieving metadata

`SmvDataSet` will provide a method `getMetadata` to get the `SmvMetadata` for an object (or perhaps just the Json string). If the metadata has already been persisted, we will essentially just read back the file. However, we will in some circumstances want metadata for modules which have not been run since they were last changed and thus do not have persisted metadata. In this case, we will construct the metadata that can be determined for a module without running it (FQN, inputs, possibly schema, etc.) and return that.

# Validation

Users may validate metadata by overriding `validateMetadata`. Returning `None` will indicate success, while returning a string will indicate failure - the string should be a message describing a failure. Metadata validation directly follows DQM validation. 