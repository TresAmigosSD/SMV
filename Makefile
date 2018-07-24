SHELL := /bin/bash

SPARKS_DIR = .sparks

SPARK_VERSIONS = $(shell cat admin/.spark_to_test)
DEFAULT_SPARK = $(shell tail -1 admin/.spark_to_test)

SPARK_HOMES = $(addprefix $(SPARKS_DIR)/, $(SPARK_VERSIONS))
DEFAULT_SPARK_HOME = $(addprefix $(SPARKS_DIR)/, "$(DEFAULT_SPARK)")

PYTHON_VERSIONS = $(shell cat admin/.python_to_test)
DEFAULT_PYTHON_PATCH = $(shell tail -1 admin/.python_to_test)
DEFAULT_PYTHON_MAJOR = $(shell cut -c 1-3 <<< $(DEFAULT_PYTHON_PATCH))

SMV_VERSION = v$(shell cat .smv_version)

SMV_ROOT = $(shell pwd)

clean:
	rm -rf .tox
	rm -f $(BUNDLE_NAME)
	sbt clean

install: install-basic

install-basic: install-spark-default assemble-fat-jar

install-full: install-spark-all assemble-fat-jar

assemble-fat-jar:
	sbt assembly

publish-scala: assemble-fat-jar
	sbt publish-local


BUNDLE_NAME = smv_$(SMV_VERSION).tgz
BUNDLE_PATH = docker/smv/$(BUNDLE_NAME)
BUNDLE_EXCLUDE = venv metastore_db .tox .ivy2 $(SPARKS_DIR) .git admin $(BUNDLE_NAME)

local_bundle:
	# cleanup some unneeded binary files.
	rm -rf project/target project/project
	rm -rf target/resolution-cache target/streams
	# use the `find ... -exec` variant instead of xargs
	# because we don't want `rm` to execute if `find` returns nothing
	find target -name *with-dependencies.jar -prune -o -type f -exec rm -f \{\} +
	find src -name '*.pyc' -exec rm -f \{\} +
	find src -name '__pycache__' -exec rm -rf \{\} +

	tar zcvf $(SMV_ROOT)/../$(BUNDLE_NAME) -C $(SMV_ROOT)/.. $(addprefix --exclude=, $(BUNDLE_EXCLUDE)) $(shell basename $(SMV_ROOT))
	mv $(SMV_ROOT)/../$(BUNDLE_NAME) $(SMV_ROOT)/$(BUNDLE_NAME)


DOCKER_BASE_NAME = local-smv-base-$(SMV_VERSION)
DOCKER_SMV_NAME = local-smv-$(SMV_VERSION)
DOCKERFILE = docker/smv/Dockerfile

docker_base: 
	docker build --build-arg PYTHON_VERSION=$(DEFAULT_PYTHON_PATCH) --target smv-build \
		-t $(DOCKER_BASE_NAME) -f $(DOCKERFILE) .

release_bundle: docker_base
	docker run -v $(shell pwd):/mount $(DOCKER_BASE_NAME) bash -c "cp /usr/lib/SMV/$(BUNDLE_NAME) /mount"

docker_smv: docker_base
	docker build --build-arg PYTHON_VERSION=$(DEFAULT_PYTHON_PATCH) -t $(DOCKER_SMV_NAME) --target smv -f $(DOCKERFILE) .

docker: docker_smv


# install-spark x.y.z
# Easier to remember than the .sparks/x.y.z that we define below
INSTALL_SPARK_RULES = $(addprefix install-spark-, $(SPARK_VERSIONS))

$(INSTALL_SPARK_RULES) : install-spark-% : $(SPARKS_DIR)/%

# .sparks/x.y.z
$(SPARK_HOMES) : $(SPARKS_DIR)/% :
	mkdir -p .sparks
	bash tools/spark-install --spark-version $* --target-dir $(SPARKS_DIR)/$* 

install-spark-default: install-spark-$(DEFAULT_SPARK)

install-spark-all: $(INSTALL_SPARK_RULES)


# Quick tests are sufficient for day-to-day dev 
test: test-quick

# Run all the basic tests tests with the default Python and Spark
test-quick: test-scala test-python test-integration

test-scala:
	sbt test

test-python: install-basic
	tox -e $(DEFAULT_PYTHON_MAJOR) -- bash tools/smv-pytest --spark-home $(DEFAULT_SPARK_HOME)

test-integration: install-basic publish-scala
	tox -e $(DEFAULT_PYTHON_MAJOR) -- bash src/test/scripts/run-integration-test.sh --spark-home $(DEFAULT_SPARK_HOME)
                               

# test-spark-x.y.z
# Run python unit tests and integrations tests against target spark versions
# TODO: is it really necessary to test every spark version with every python version
TEST_SPARK_RULES = $(addprefix test-spark-, $(SPARK_VERSIONS))

$(TEST_SPARK_RULES) : test-spark-% : install-spark-% assemble-fat-jar publish-scala
	# e.g. tox -e py26 -e py35 -- bash ...
	tox $(addprefix "-e ", $(PYTHON_VERSIONS)) -- \
		bash tools/smv-pytest --spark-home $(SPARKS_DIR)/$*
	tox $(addprefix "-e ", $(PYTHON_VERSIONS)) -- \
		bash src/test/scripts/run-integration-test.sh --spark-home $(SPARKS_DIR)/$*

# Test all supported Spark and Python versions
test-thorough: install-full test-scala $(TEST_SPARK_RULES)
