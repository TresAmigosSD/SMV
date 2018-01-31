name := "smv"

organization := "org.tresamigos"

version := "2.1-SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation", "-feature")

val sparkVersion = "2.1.1"
javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

libraryDependencies ++= Seq(
  "org.apache.spark"             %% "spark-sql"         % sparkVersion % "provided",
  "org.apache.spark"             %% "spark-hive"        % sparkVersion % "provided",
  "org.scalatest"                %% "scalatest"         % "2.2.6" % "test",
  "com.google.guava"             % "guava"              % "14.0.1",
  "com.github.jlmauduy"          %% "ascii-graphs"              % "0.0.7",
  "org.rogach"                   %% "scallop"           % "0.9.5",
  "org.joda"                     % "joda-convert"       % "1.7",
  "joda-time"                    % "joda-time"          % "2.7",
  "org.eclipse.jgit" % "org.eclipse.jgit" % "4.7.0.201704051617-r",
  "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4"
)

parallelExecution in Test := false

publishArtifact in Test := true

// fork in Test := true

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
).value

mainClass in assembly := Some("org.tresamigos.smv.SmvApp")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}-${version.value}-jar-with-dependencies.jar"

test in assembly := {}

// initialize ~= { _ => sys.props("scalac.patmat.analysisBudget") = "off" }
