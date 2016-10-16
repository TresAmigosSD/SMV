package org.tresamigos.smv;

import org.apache.spark.sql.DataFrame;

/**
 * Methods that can be implemented by a remote object, such as a
 * Python class, to allow modules written in different languages to
 * work together in an SMV application.
 */
public interface SmvModuleRepository {
	/**
	 * Does the named module exist?
	 */
	boolean hasModule(String modfqn);

	/**
	 * Try to run the module by its fully-qualified name and return its
	 * result in a DataFrame.
	 */
	DataFrame runModule(String modfqn);
}
