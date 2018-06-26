SPARKS_DIR = .sparks/

SPARK_VERSIONS = $(shell cat admin/.spark_to_test)
DEFAULT_SPARK = $(shell tail -1 admin/.spark_to_test)

SPARK_HOMES = $(addprefix $(SPARKS_DIR), $(SPARK_VERSIONS))
DEFAULT_SPARK_HOME = $(addprefix $(SPARKS_DIR), "$(DEFAULT_SPARK)")

DEFAULT_TOX_ENV = py35


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

$(INSTALL_SPARK_RULES) : install-spark-% : $(SPARKS_DIR)%

$(SPARK_HOMES) : .sparks/% :
	mkdir -p .sparks
	bash tools/spark-install --spark-version $* $(SPARKS_DIR)$*


test: test-quick

test-quick: test-scala test-python test-integration

test-scala:
	sbt test

test-python: install-basic
	tox -e $(DEFAULT_TOX_ENV) -- bash tools/smv-pytest --spark-home $(DEFAULT_SPARK_HOME)

test-integration: install-basic publish-scala
	tox -e $(DEFAULT_TOX_ENV) -- bash src/test/scripts/run-integration-test.sh --spark-home $(DEFAULT_SPARK_HOME)



TEST_SPARK_RULES = $(addprefix test-spark-, $(SPARK_VERSIONS))

test-thorough: install-full test-scala $(TEST_SPARK_RULES)

$(TEST_SPARK_RULES) : test-spark-% : install-spark-% assemble-scala publish-scala
	tox -- tools/smv-pytest --spark-home $(SPARKS_DIR)$* && \
		   bash src/test/scripts/run-integration-test.sh --spark-home $(SPARKS_DIR)$*





