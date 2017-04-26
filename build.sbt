name := "smv"

organization := "org.tresamigos"

version := "1.5-SNAPSHOT"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-deprecation", "-feature")

val sparkVersion = "1.5.2"

libraryDependencies ++= Seq(
  "org.apache.spark"             %% "spark-sql"         % sparkVersion % "provided",
  "org.apache.spark"             %% "spark-hive"        % sparkVersion % "provided",
  "org.scalatest"                %% "scalatest"         % "2.2.0" % "test",
  "com.google.guava"             % "guava"              % "14.0.1",
  "org.rogach"                   %% "scallop"           % "0.9.5",
  "org.joda"                     % "joda-convert"       % "1.7",
  "joda-time"                    % "joda-time"          % "2.7",
  "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.2",
  "com.github.mdr"               %% "ascii-graphs"      % "0.0.6"
)

parallelExecution in Test := false

publishArtifact in Test := true

// Once upgraded to sbt 1.0.0 (even milestones) this hack
// is no longer necessary. Note that this command is present irrespective of the
// `scalafmt on compile` task that is present in the ScalafmtOnCompile plugin, and
// accepts normal scalafmt arguments.
commands += Command.args("scalafmt", "Run scalafmt cli.") {
  case (state, args) =>
    for {
      scalafmt <- ScalafmtOnCompilePlugin.getLatestScalafmt().right
    } yield scalafmt.main("--non-interactive" +: args.toArray)
    state
}

// Create itest task that runs integration tests
val itest = TaskKey[Unit]("itest", "Run Integration Test")
itest := {
  assembly.value
  publishLocal.value
  val res = ("src/test/scripts/run-integration-test.sh" !)
  if (res > 0) throw new IllegalStateException("integration test failed")
}

// Create pytest task that runs the Python unit tests
val pytest = TaskKey[Unit]("pytest", "Run Python Unit Tests")
pytest := {
  assembly.value
  val res = ("tools/smv-pytest" !)
  if (res > 0) throw new IllegalStateException("pytest failed")
}

// Create alltest task that sequentially runs each test suite
val allTest = TaskKey[Unit]("alltest", "Run All Test Suites")
allTest := Def.sequential(
  test in Test,
  pytest,
  itest
)

mainClass in assembly := Some("org.tresamigos.smv.SmvApp")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}-${version.value}-jar-with-dependencies.jar"

test in assembly := {}
