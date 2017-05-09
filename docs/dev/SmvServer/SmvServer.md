# Problem
When running modules by command line, a Spark context will be created and destroyed repeatedly, which brings too much overhead.
Ideally, we should create a SMV server where a SMV runtime is maintained and users can execute SMV operations by calling the server's API.

# Requirements
A set of APIs should be created for the following operations:
* run modules
* get module's code
* get module's sample output
* get module's schema
* get the project's dependency graph JSON

# Design
The singleton `smvPy` of class `SmvPy` will be imported at the start of the server. `smvPy` holds the following private instances:
* an instance of `SparkContext`
* an instance of `HiveContext`
* an instance of `SmvPyClient`, which contains an instance `j_smvApp` of class `SmvApp` (the entrance of SMV app)

The SMV operations will be actually handled by `j_smvApp`.
