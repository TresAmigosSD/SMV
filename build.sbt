name := "smv"

organization := "org.tresamigos"

version := "2.1-SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation", "-feature")

val sparkVersion = "2.1.0"

val jettyVersion = "8.1.18.v20150929"

val commonsHttpclientVersion = "4.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark"             %% "spark-sql"         % sparkVersion % "provided",
  "org.apache.spark"             %% "spark-hive"        % sparkVersion % "provided",
  "org.scalatest"                %% "scalatest"         % "2.2.6" % "test",
  "com.google.guava"             % "guava"              % "14.0.1",
  "org.rogach"                   %% "scallop"           % "0.9.5",
  "org.eclipse.jetty"            % "jetty-server"       % jettyVersion,
  "org.eclipse.jetty"            % "jetty-client"       % jettyVersion,
  "org.apache.httpcomponents"    % "httpclient"         % commonsHttpclientVersion,
  "org.joda"                     % "joda-convert"       % "1.7",
  "joda-time"                    % "joda-time"          % "2.7",
  "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4"
)

parallelExecution in Test := false

publishArtifact in Test := true

fork in Test := true

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
allTest := {
  (test in Test).value
  pytest.value
  itest.value
}

mainClass in assembly := Some("org.tresamigos.smv.SmvApp")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}-${version.value}-jar-with-dependencies.jar"

test in assembly := {}
