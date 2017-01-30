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
 * Repository methods used to query and instantiate modules
 * implemented in languages other than Scala.
 */
public interface SmvDataSetRepository {
	/**
	 * Does the named data set exist?
	 */
	boolean hasDataSet(String modUrn);

	/**
	 * Factory method for ISmvModule
	 */
	ISmvModule getSmvModule(String modUrn);

	/**
	 * Urns of output module for a stage in this repository
	 */
	String[] outputModsForStage(String stageName);

	/**
	 * Re-run the named module after code change.
	 */
	DataFrame rerun(String modUrn, DQMValidator validator, Map<String, DataFrame> known);
}
