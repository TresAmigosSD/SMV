# SMV class loader client/server

## Introduction
When developing modules, it is currently too arduous to compile/jar/restart the shell/server to see the effect of the new module.
Ideally, we should be able to "reload" a newly created/modified SmvModule without having to restart the server, create a new context and re-cache the data.
The data cache issue can be taken care of by the Spark Server project which maintains a single spark context instance across invocations.  However, that solution does not address the issue of module modification as usually occurs during development.
The SMV class loader allows for the serving of class data using a client/server architecture and utilizing a custom class loader on the client side (shell/server).  This allows for dynamic reloading of client code (e.g. SmvModule/SmvFile/utility classes).  This in turn reduces development cycle considerably.

## Configuration
The following parameters would be added to standard SmvApp config parameters.

* `smv.class_server.host` : host name of class server.  If this is specified, then the dynamic class loader server will not be utilized and SMV will look for module classes on the server
* `smv.class_server.port` : port number where class server will be listening for connections and where SmvApp will connect to.  Defaults to 9900.
* `smv.class_server.class_dir` : directory where the class server would look for new class instances.  If this is specified but host was not specified, then the dynamic class loader is run in standalone mode (that is, a server instance is not required and the new class files are looked up directly from this app)

If neither `host`, nor `class_dir` are specified, then SMV will revert to standard CLASSPATH lookup (using `Class.forName()` method) on default class loader.

## Running
In order to utilize the SMV class server, the server instance must be started.  A convenience script is provided in `SMV/tools` directory to launch the server.  The server **MUST** be launched from the top level of an application directory.

```shell
$ ${SMV_HOME}/tools/smv-class-server
```
