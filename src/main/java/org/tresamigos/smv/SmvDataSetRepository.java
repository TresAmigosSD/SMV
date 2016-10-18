package org.tresamigos.smv;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.DataFrame;

/**
 * Methods that can be implemented by a remote object, such as a
 * Python class, to allow modules written in different languages to
 * work together in an SMV application.
 */
public interface SmvDataSetRepository {
	/**
	 * Does the named data set exist?
	 */
	boolean hasDataSet(String modfqn);

	/**
	 * A CSV of dependent module fqns or an empty string.
	 *
	 * Using a csv string is a temporary workaround until we can solve
	 * the issue of type conversion between Python and Java VMs.
	 */
	String dependencies(String modfqn);

	/**
	 * Try to run the module by its fully-qualified name and return its
	 * result in a DataFrame.
	 */
	DataFrame getDataFrame(String modfqn, Map<String, DataFrame> modules);
}
