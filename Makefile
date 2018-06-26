SPARKS_DIR = .sparks

SPARK_VERSIONS = $(shell cat admin/.spark_to_test)
DEFAULT_SPARK = $(shell tail -1 admin/.spark_to_test)

SPARK_HOMES = $(addprefix $(SPARKS_DIR)/, $(SPARK_VERSIONS))
DEFAULT_SPARK_HOME = $(addprefix $(SPARKS_DIR)/, "$(DEFAULT_SPARK)")

PYTHON_VERSIONS = $(shell cat admin/.python_to_test)
DEFAULT_PYTHON_VERSION = $(shell tail -1 admin/.python_to_test)


install : install-basic

install-basic : install-spark-default assemble-scala

install-full : install-spark-all assemble-scala

assemble-scala:
	sbt assembly

publish-scala: assemble-scala
	sbt publish-local


install-spark-default: install-spark-$(DEFAULT_SPARK)

INSTALL_SPARK_RULES = $(addprefix install-spark-, $(SPARK_VERSIONS))

install-spark-all: $(INSTALL_SPARK_RULES)

$(INSTALL_SPARK_RULES) : install-spark-% : $(SPARKS_DIR)/%

$(SPARK_HOMES) : .sparks/% :
	mkdir -p .sparks
	bash tools/spark-install --spark-version $* $(SPARKS_DIR)/$*


test: test-quick

test-quick: test-scala test-python test-integration

test-scala:
	sbt test

test-python: basic-install
	tox -e $(DEFAULT_PYTHON_VERSION) -- bash tools/smv-pytest --spark-home $(DEFAULT_SPARK_HOME)

test-integration: install-basic publish-scala
	tox -e $(DEFAULT_PYTHON_VERSION) -- bash src/test/scripts/run-integration-test.sh --spark-home $(DEFAULT_SPARK_HOME)
                               

TEST_SPARK_RULES = $(addprefix test-spark-, $(SPARK_VERSIONS))

test-thorough: install-full test-scala $(TEST_SPARK_RULES)

$(TEST_SPARK_RULES) : test-spark-% : install-spark-% assemble-scala publish-scala
	# e.g. tox -e py26 -e py35 -- bash ...
	tox $(addprefix "-e ", $(PYTHON_VERSIONS)) -- \
		bash tools/smv-pytest --spark-home $(SPARKS_DIR)/$*
	tox $(addprefix "-e ", $(PYTHON_VERSIONS)) -- \
		bash src/test/scripts/run-integration-test.sh --spark-home $(SPARKS_DIR)/$*


