# Getting started with SMV development

## Setup

### Dependencies

The complete SMV development lifecycle requires

* Java 7 or 8
* Python 2.7, 3.5, *and* 3.6 (for compatibility testing)
* make
* sbt
* tox
* docker
* git (duh)

Install the distros of your choice however you like. For Python version management, the core developers typically use pyenv. 

### Verifying your environment

To verify your environment is set up properly, run 

```
make test-thorough
```

This will install all required versions of Spark, assemble the fat jar, and run all of SMV's tests. The first time you invoke it may take you in the are of 45 minutes. Subsequent invocations should take in the area of 5 minutes.

## Testing

`make test` is sufficient validation for a typical development iteration - a more thorough set of tests will run in the CI. When appropriate the following targets can be used:

### Quick testing:

* test-scala: run the scala unit tests
* test-python: run the python unit tests
* test-integration: run the integration tests

### Thorough testing

* test-spark-*: test against a specific Spark version with all Python versions
* test-thorough: run **all** tests for **all** Spark versions and **all** Python versions
