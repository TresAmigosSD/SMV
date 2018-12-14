# Use Atom Editor with SMV Projects

Atom is a light weight editor/IDE which is highly extendable with a lot of packages. Given the ad-hoc
nature of data application development which SMV, we need quickly switch between editor/IDE and shell
terminals either on local or remote. Atom is an ideal editor for that purposes.

When you installed Atom, you can install some useful packages through the setting dialog window (which will show up when you type Cmd-, ).

## Some Useful Packages

* vim-mode & ex-mode - In case you are a VIM lover
* language-scala - Scala highlighter (This module is only a highlighter, has no knowledge on semantics)
* language-plantuml - Use plantUML for class diagrams and table ER diagrams? This is the highlighter
* plantuml-viewer - Preview plantUML plots (need Graphviz, `dot` command, on system)

In case you really need Atom to behave like an Scala IDE, you need the `Ensime` package. Will cover that
topic later.

## Work on Remote code
There are multiple packaged for Atom to work on remote code. After tried several of them, I found that for most of SMV use cases, the `atom-sync` package is pretty good.
https://atom.io/packages/atom-sync
The following is my ".sync-config.cson" file in a typical project

```json
remote:
  host: "rserver"
  user: "mynameonremote"
  path: "/home/users/mynameonremote/myProject"
behaviour:
  uploadOnSave: true
  syncDownOnOpen: true
  forgetConsole: false
  autoHideConsole: true
  alwaysSyncAll: false
option:
  deleteFiles: false
  exclude: [
    ".sync-config.cson"
    "target"
    "conf/smv-user-conf.props"
    "metastore_db"
    "dist"
  ]
```

Please note that I configured private-public key pair for "ssh" so that "rsync" can run quietly.
Also I utilized `.ssh/config` to specify server ssh connection details. I aliased my server as
`rserver` in the config file. For details please refer
http://nerderati.com/2011/03/17/simplify-your-life-with-an-ssh-config-file/

My `.ssh/config` example:
```
host rserver
    User ninjapapa
    Hostname myserver.mycompany.com
```

With `atom-sync`, whenever you save on local version, it will `rsync` to the remote copy, so that
the experience is pretty similar to editing the remote file directly.

Here are the steps of using `atom-sync`,
* On sever, `git clone` the project
* On local machine, create a project folder, and edit `.sync-config.cson` as discribed above
* Launch `atom` (with `atom-sync` installed) on the local project folder
* Right click on the project folder and sync from remote to local
* Edit on the code, when save code, it will be synced up to the server

Recommend to do all the `git` operation on the server, and let `atom-sync` to sync down the `.git` directory
also (does not put `.git` in the `exclude` list in the config file).

Combining using this, SBT continuous compile, SMV class loader in SMV shell on the remote server,
you can simplify the entire edit-compile-try cycle of SMV Module development.

## Use Snippets
One can add the following to ~/.atom/snippets.cson

```json
'.source.scala':
  'smv.smvModule':
    'prefix': 'xsmvm'
    'body': """
object ${1:ExampleSmvModule} extends
  SmvModule("${2:ExampleSmvModule Description}") {

  override def version() = 0
  override def requiresDS() = Seq(
    ${3:List_of_SmvDataSet_This_module_depends}
  )

  override def run(i: runParams) = {
    val df = i(${4:ModuleDependOn})
    import df.sqlContext.implicits._
    ${5}
  }
}

"""
```

Now when you edit a Scala file, when you type `xsmvm` and the tab key, you will have a template
SmvModule to start with.

## Use Atom with Ensime as a Scala IDE
Ensime run on SBT as a server analyzing Scala code and serves all type of editors/IDEs with Scala
specific IDE functions.

Atom has an Ensime package to connect to the Ensime server.

Here are the steps to make it work

* Install Ensime plugin: follow the README https://github.com/ensime/ensime-atom
* To generate .ensime file in project dir
 * Install sbt, if not installed yet
 * modify `~/.sbt/0.13/plugins/build.sbt` with `addSbtPlugin("org.ensime" % "ensime-sbt" % "0.2.1")`
 * run `sbt gen-ensime` in your project dir
* You also need to compile the project with sbt, run "sbt package" in your project dir
* Start atom on the project
* Start ensime server with cmd-shift-P Ensime: Start

The first time it will take quite long to get the server running, since it will download a lot stuff.
The 2 addition plugins mentioned in ensime-atom README are very helpful too.

We typically need full IDE functionality (basically jumping around class/function definition and
uses) when we work on core SMV development. In most of app development using SMV, we more likely
to work on code and the interactive shell together, in that case basic Atom navigation without
Ensime is good enough.
