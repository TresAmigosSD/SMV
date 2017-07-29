/*
 * This file is licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tresamigos.smv;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.DataFrame;
import org.tresamigos.smv.dqm.SmvDQM;
import org.tresamigos.smv.dqm.DQMValidator;

/**
 * Methods that can be implemented by a remote object, such as a
 * Python class, to allow modules written in different languages to
 * work together in an SMV application. If you add a method here with
 * a Python implementation *make sure* to use the @with_stacktrace
 * decorator to ensure that errors that occur in callbacks don't get eaten.
 */
public interface ISmvModule {
	/**
	 * Does the result of this module need to be persisted?
	 *
	 * Input datasets and simple filter and map modules typically don't
	 * need to be persisted.
	 */
	IPythonResponsePy4J<Boolean> getIsEphemeral();

  /**
   * override sql query to use when publishing to a hive table.
   */
  IPythonResponsePy4J<String> getPublishHiveSql();

  /** DataSet type: could be 3 values, Input, Link, Module */
	IPythonResponsePy4J<String> getDsType();

	IPythonResponsePy4J<Boolean> getIsOutput();

	IPythonResponsePy4J<String> getTableName();

	IPythonResponsePy4J<String> getFqn();

	/**
	 * The attached DQM policy + any type specific policies.
	 */
	IPythonResponsePy4J<SmvDQM> getDqmWithTypeSpecificPolicy();

	/**
	 * Dependent module fqns or an empty array.
	 *
	 * Python implementation of this method needs to return a Java array
	 * using the accompanying smv_copy_array() method.
	 */
	IPythonResponsePy4J<String[]> getDependencyUrns();

	/**
	 * Try to run the module by its fully-qualified name and return its
	 * result in a DataFrame.
	 */
	IPythonResponsePy4J<DataFrame> getGetDataFrame(DQMValidator validator,  Map<String, DataFrame> known);

	/**
	 * Hash computed based on the source code of the dataset's class
	 */
	IPythonResponsePy4J<Integer> getSourceCodeHash();

	/**
	 * Hash computed based on instance values of the dataset, such as the timestamp
	 * of an input file
	 */
	IPythonResponsePy4J<Integer> getInstanceValHash();
}
