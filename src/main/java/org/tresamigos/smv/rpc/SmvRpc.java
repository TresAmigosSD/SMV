package org.tresamigos.smv.rpc;

import org.apache.spark.sql.DataFrame;

/**
 * Methods that can be implemented by a remote object, such as a
 * Python class, to allow modules written in different languages to
 * work together in an SMV application.
 */
public interface SmvRpc {
	String hi(String modname);
	DataFrame runModule(String modname);
}
