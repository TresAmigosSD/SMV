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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
	boolean isEphemeral();

  /**
   * override sql query to use when publishing to a hive table.
   */
  String publishHiveSql();

  /** DataSet type: could be 3 values, Input, Link, Module */
	String dsType();

	boolean isOutput();

	String tableName();

	String fqn();

	/**
	 * The attached DQM policy + any type specific policies.
	 */
	SmvDQM dqmWithTypeSpecificPolicy();

	/**
	 * Dependent module fqns or an empty array.
	 *
	 * Python implementation of this method needs to return a Java array
	 * using the accompanying smv_copy_array() method.
	 */
	String[] dependencyUrns();

	/**
	 * Try to run the module by its fully-qualified name and return its
	 * result in a DataFrame.
	 */
	Dataset<Row> getDataFrame(DQMValidator validator,  Map<String, Dataset<Row>> known);

	/**
	 * Hash computed based on the source code of the dataset's class
	 */
	int sourceCodeHash();

	/**
	 * Hash computed based on instance values of the dataset, such as the timestamp
	 * of an input file
	 */
	int instanceValHash();
}
