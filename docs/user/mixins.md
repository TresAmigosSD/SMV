# Using Mixins

## Reporting Python tracebacks in functions overridden by the user
Some function implementations in the SmvDataSet class can be overridden by the users in their own SMV modules (e.g. isEphemeral).

If there are errors in the user's function, no Python stack trace is printed, because the python function is called through Py4J and the ISmvModule interface.

In order to print the stack trace, we override the `__getattribute__` function to intercept all attribute calls, and wrap the function execution in a try-except block.  The base class' function must be decorated with the `with_stacktrace` decorator.

The decorator marks the function's name as a function that needs the printing of a stack trace.

## Potential issues
This may lead to two issues:
 - if two classes extend the `WithStackTrace` mixin and both define a function `foo`, and `foo` is decorated in only one of the two classes, then the stack trace will be printed for both functions. The downside here is that worst case, a stack trace may be printed twice.

 - upon renaming functions, ensure that the correct functions are still decorated.

## Proper resolution
The above issues can be resolved by checking instead of just the function name, the function class as well.  This may require traversing the MRO (method resolution order), and verify that the function is indeed decorated.  Such traversal may be expensive, but can be optimized to run only once per module per instance, by adding a flag to the class objects.

## Decision
After evaluation of the risks raised by the potential issues above, it was determined that the complexity of implementation may be too much work to mitigate such low risk.
