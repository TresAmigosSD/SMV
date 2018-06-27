# Building with SBT

Most users _will not_ need to compile anything to run an SMV application - for pure Python projects, the user can just run their application against a prebuilt binary. For those who do need to build SMV themselves, this document will review the use of [SBT](http://www.scala-sbt.org/download.html) for building SMV. SBT is the preferred tool for building SMV.

### Build the SMV fat JAR
```
$ sbt assembly
```

## Memory options
If you are using Java 8, you may encounter `OutOfMemoryErrors` when running the Scala unit tests. This is because if you don't specify memory options to SBT yourself, SBT will set them for you and set them very small. You can specify your own memory setting by setting the `-J-XX:MaxMetaspaceSize` option in `/usr/local/etc/sbtopts`. Setting it to 1024m (i.e. `-J-XX:MaxMetaspaceSize=1024m`) should be sufficient. Alternatively, you can specify this setting in the `SBT_OPTS` environment variable with `export SBT_OPTS="-J-XX:MaxMetaspaceSize=1024m $SBT_OPTS"`
