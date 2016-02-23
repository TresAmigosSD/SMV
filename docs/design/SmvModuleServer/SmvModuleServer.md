# Problem
When developing modules, it is currently too arduous to compile/jar/restart the shell/server to see the effect of the new module.
Ideally, we should be able to "reload" a newly created/modified SmvModule without having to restart the server, create a new context and recache the data.
The data cache issue can be take care of by the Spark Server project which maintains a single spark context instance across invokations.  However, that solution does not address the issue of module modification as usually occurs during development.

# Requirements
* User should be able to "run" a new/modified module
* Must be able to be utilized on a cluster environment (must work with multiple executors)
* Must work efficiently in standalone mode.  Ideally, user should not have to start/maintain the server independent of the shell/cmd run.

# Design
Due to need to support invocation on a cluster environment, a client/server architecture will be utilized for the solution.
The server will maintain a collection of latest/greatest instances of a classes.  The client would request the class code from the server using the FQN of the class name.

`SmvApp` would need to be modified so that it would not use `Class.forName` to instantiate an instance of an `SmvModule`, but rather load the class code from the module server and instantiate the class instance using a custom class loader.

## Config
The following parameters would be added to standard SmvApp config parameters.

* `smv.mod_server.host` : host name of module server.  If this is not specified, then the dynamic module server will not be utilized and SMV will look for modules in the normal CLASSPATH (using `Class.forName()` method).
* `smv.mod_server.port` : port number where module server will be listening for connections and where SmvApp will connect to.  Defaults to XXXX.
* `smv.mod_server.class_dir` : directory where the module server would look for new class instances.

## Class Loader
We must use a custom class loader to load the class code from the server on the client side.  We must use a new instance of the class loader to load a new instance of a class as a single class loader can not have multiple instances of the same class code.  We can dynamically create a new class loader when needed (when all current class loaders have a version of the desired new class instance), or for simplicity sake, create a new instance of the class loader right away.

The recommend process for overloading `loadClass` method in a custom class loader is to defer the loading to the parent class loader first and then only take the custom action when needed.  We may need to reverse the order here because the jar file could contain an old version

Note that most SmvModules are implemented as objects.  Scala objects map to two java classes. Thus, we may need to load both classes (X and X$) when loading module X.  For the sake of the discussion below, we only refer to the class X, but in reality, caching, transport, version, etc are actually refering to the class pair (should it?).

As per design doc review, a couple of discoveries were made.  We want to minimize the loading of classes from the module server.  The module server should ideally only serve SmvModule objects.  A couple of methods were considered to accomplish that:


* Custom class loader always loads from parent first and get class from server if parent (default) loader doesn't find the class.  This is the standard ClassLoader behaviour.  However, if the parent has access to app fat jar, then the app will find the desired class and will use the one from the jar rather than from the build dir (from server).  It is nearly impossible to create an app fat jar that does not contain classes derived from `SmvModule`.
* Custom class loader behaves in standard matter as desribed above but if parent loader finds a class that inherits from from `SmvModule`, it will discard it and request the class from the server.  This will mean only `SmvModule` derived classes would be requested from server.  But, this comes at the cost of having to load modules twice (once by parent and once by the server).
* Custom class loader can go to server first and then to parent if class not found on server.  This could create quite a bit of traffic to server as every class used by app would be attempted to be loaded by server (server will not find it, but still have to pay the overhead of request per class).  We can reduce the traffic by creating a common "blacklist" of classes such as "java.util.*" that will never be loaded from server.  This is not very sustainable as users add new libraries, they will forget to update the "blacklist" which causes slowdown on startup of app.
* Custom loader can become generic class server and app is only linked against core SMV and does not have access to any jar/classes from app (so no SmvModule/File instances).  Class loader would then also need to handle resource loading (the other part of standard class loader interface).  The custom class loader would then defer to parent first (as recommended by java) and only go to server for app specific classes/resources.  We can also create an "initial" load of server classes so to avoid having to make a separate request per class on app init.  All classes known by server would be bundled and shipped to app in a single request.  Post init period, all loadClass operations would defer to server as stated above.  This is purely an optimization.

The decision is to have the custom class loader load from server first and only go to parent if not found.  This is needed because there is no way to avoid using the fat jar as third party libraries are only included in the fat jar and there is no way to build a fat jar without the app smv modules.  However, this custom loader will **only** be used to load SmvModules in app/shell.  We can also "whitelist" some common "java.lang.*" and other packages to avoid the trip to the server.

## Class Serialization
While it is possible for the server to create the class instance (not object) and then serialize it (because java Class is serializable), the module server will only be concerned with serving the raw bytes of ".class" files.  This is done so that the client can utilize the standard `defineClass` method of the `ClassLoader` to create the class on the client side.

## Server Cache Refresh
The actual cache refresh policy is still TBD.  We need to do some performance testing to determine the best path to go.  There are two options:

* Only do a scan of the `class_dir` when user explicitly calls `UpdateCache` entry point.  After that, even if the underlying ".class" file changes, it will not be reloaded.
* Always check the timestamp of the underlying ".class" file.  If the timestamp has changed, then we must reload the new version and serve that as the current version of truth. Note: need to worry about sync issue if the class is being written as we are reading it and make sure that all instances of spark executor get the same version.

We will probably implement option 1 first as it is simple and do some performance measure to see if option 2 is viable (technical issues aside).

## Client
On the client side, `SmvApp` will maintain a single connection to the server that can be used by multiple class loaders to avoid overhead of creating connection per request.

If the server name was not specified in the config, then the client is assumed to be in stand-alone mode (perhaps we should determine that from the spark master parameter instead?).  In stand-alone mode, the dynamic loading is performed as expected but there will be no client/server communication. `SmvModuleClassLoader` will utilize the `ClassCache` class instance directly instead of going over the network by using `LocalModuleServerConnection`.

## Client Side Cache
There is no need for a cache of class code on the client side.  `SmvApp` already caches the result of the resolveRDD so we normally only instantiate the class instance once anyway.

## Protocol
1. GetClassCode
  * input: full_class_name, class_version
  * return: byte code of corresponding ".class" file.  If the current class version is same as provided class_version, then a special "has_latest" error is returned to indicate to the client that it already has the latest.  This will allow us to use a single call to check for latest version and get it if we have a stale version.

2. UpdateCache (optional)
  * input: None
  * return: None
  * description: Check for updated classes in `class_dir`.
  Reload classes from the configured `class_dir`.  This may be replaced by a time stamp check in the future so no explicit refresh is required.  However, it might be handy to keep the "refresh" command to clear the cache of old unused classes.

# Future Enhancements
* Governance : we can add rules about who can load/run a module.
* Run statistics : we can track run statistics at the module level.  Who, when, how often a module was run.
* multi versions : we can add support for multiple versions of the same module and allow the user to ask for specific version.
