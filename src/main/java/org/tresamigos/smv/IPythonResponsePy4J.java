package org.tresamigos.smv;

public interface IPythonResponsePy4J<T> {
  boolean successful();
  T response();
}
