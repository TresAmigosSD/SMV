# Issues Reloading Python Code

There are currently several issues (#641, #642, #646, etc.) which result from complications in managing the reloading of client Python code within a transaction. We could solve each of them individually, introducing additional nuances and complications to the process of loading modules, but it is preferable to come to a more general solution both for simplicity and to prevent similar future problems.

# Desired Behavior

Within a transaction, we want every module needed within the transaction to be loaded from the most recent source _once_ (#646 has illustrates a problem that can arise from loading a module twice in a transaction).

# Current Approach

We currently approximate this behavior by reloading a module using the builtin `reload` function every time we load one of its member `SmvDataSets` (putting it briefly). Complications have arisen when we want to assure that the module is loaded only once, when we want to make sure that inheritance dependencies are also reloaded, etc.

# Alternative Approach

 Essentially, during each transaction client code should be loaded as though it was run in a fresh interpreter. We should be able to accomplish this by removing all client modules from `sys.modules` at the beginning of each transaction (a similar strategy is outlined [here](https://www.safaribooksonline.com/library/view/python-cookbook/0596001673/ch14s02.html)). We may also need to suppress creation of `.pyc` files (which would also solve #612). We also will need to investigate if this has unintended consequences for the linecache. However, the objective of this approach is to force the interpreter to forget about client modules it imported before the current transaction, compile them the first time they are imported during the current transaction, and import them without compilation every subsequent time they are imported with the current transaction.
