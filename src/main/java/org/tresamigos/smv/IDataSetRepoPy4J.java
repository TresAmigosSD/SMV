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

/**
 * Repository methods used to query and instantiate modules
 * implemented in languages other than Scala. If you add a method here with
 * a Python implementation *make sure* to use the @with_stacktrace
 * decorator to ensure that errors that occur in callbacks don't get eaten.
 */
public interface IDataSetRepoPy4J {
	/**
	 * Factory method for ISmvModule
	 */
	ISmvModule loadDataSet(String modUrn);

	String[] dataSetsForStage(String modUrn);
}
