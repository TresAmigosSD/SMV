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
 * work together in an SMV application.
 */
public interface SmvDataSetRepository {
	/**
	 * Does the named data set exist?
	 */
	boolean hasDataSet(String modUrn);

	/**
	 * Is the named dataset external?
	 */
	boolean isExternal(String modUrn);

	/**
	 * Return the name of the external dataset if the name links to one;
	 * otherwise return an empty string.
	 */
	String getExternalDsName(String modUrn);

	/**
	 * Is the named dataset a link to a published module?
	 */
	boolean isLink(String modUrn);

	/**
	 * Return the name of the target dataset to which a named dataset
	 * links to; otherwise return an empty string.
	 */
	String getLinkTargetName(String modUrn);

	/**
	 * Does the named dataset need to be persisted?
	 *
	 * Input datasets and simple filter and map modules typically don't
	 * need to be persisted.
	 */
	boolean isEphemeral(String modUrn);

	/**
	 * The DQM policy attached to a named dataset.
	 */
	SmvDQM getDqm(String modUrn);

	/**
	 * A CSV of output module fqns for a stage.
	 */
	String outputModsForStage(String stageName);

	/**
	 * A CSV of dependent module fqns or an empty string.
	 *
	 * Using a csv string is a temporary workaround until we can solve
	 * the issue of type conversion between Python and Java VMs when it
	 * comes to a Python class that implements a Java interface.
	 */
	String dependencies(String modUrn);

	/**
	 * Try to run the module by its fully-qualified name and return its
	 * result in a DataFrame.
	 */
	DataFrame getDataFrame(String modUrn, DQMValidator validator,  Map<String, DataFrame> known);

	/**
	 * Re-run the named module after code change.
	 */
	DataFrame rerun(String modUrn, DQMValidator validator, Map<String, DataFrame> known);

	/**
	 * Calculate a hash for the named data set; can optionally include
	 * the hash for all its super classes up to and excluding the base
	 * class provided by SMV.
	 */
	int datasetHash(String modUrn, boolean includeSuperClass);
}
