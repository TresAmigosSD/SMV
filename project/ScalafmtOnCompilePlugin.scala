import org.scalafmt.bootstrap.ScalafmtBootstrap
import sbt._
import sbt.Keys._
import sbt.Def.Initialize
import sbt.inc.Analysis

/**
  * The ScalafmtOnCompilerPlugin adds an additional step before `compile` task
  * in Compile and Test scopes.
  * The plugin 1) fetches, if necessary, a specific version of Scalafmt,
  * collects all files that have been changed, based on the cached information
  * about the state of the project, and 3) runs scalafmt on the selected files.
  * No prior configuration by the developer is necessary.
  *
  * Based on https://gist.github.com/hseeberger/03677ef75bfadb7663c3b41bb58c702b
  * Refactored to keep classfile names shorter (important for encrypted fs).
  */
object ScalafmtOnCompilePlugin extends AutoPlugin {

  private val latestScalafmt = "0.7.0-RC1"

  def getLatestScalafmt(): Either[Throwable, ScalafmtBootstrap] =
    org.scalafmt.bootstrap.ScalafmtBootstrap.fromVersion(latestScalafmt)

  object autoImport {

    private def format(handler: Set[File] => Unit, msg: String)(cache: java.io.File, sources: Set[java.io.File], sF: TaskStreams, projRef: ProjectReference) = {
      def update(handler: Set[File] => Unit, msg: String)(
        in: ChangeReport[File], out: ChangeReport[File]) = {
        val label = Reference.display(projRef)
        val files = in.modified -- in.removed
        Analysis
          .counted("Scala source", "", "s", files.size)
          .foreach(count => sF.log.info(s"$msg $count in $label ..."))
        handler(files)
        files
      }
      FileFunction.cached(cache)(FilesInfo.hash,
        FilesInfo.exists)(update(handler, msg))(sources)
    }
    private def formattingHandler(files: Set[File]) =
      if (files.nonEmpty) {
        val filesArg = files.map(_.getAbsolutePath).mkString(",")
        for {
          scalafmt <- getLatestScalafmt().right
        } yield scalafmt.main(List("--non-interactive", "-i", "-f", filesArg))
      }

    private lazy val scalafmtIncImpl: Initialize[Task[Unit]] = {
      sbt.Def.task {
        val cache = streams.value.cacheDirectory / "scalafmt"
        val include = includeFilter.in(scalafmtInc).value
        val exclude = excludeFilter.in(scalafmtInc).value
        val sources =
          sourceDirectories
            .in(scalafmtInc)
            .value
            .descendantsExcept(include, exclude)
            .get
            .toSet

        format(formattingHandler, "Formatting")(cache, sources, streams.value, thisProjectRef.value)
        format(_ => (), "Reformatted")(cache, sources, streams.value, thisProjectRef.value) // Recalculate the cache
      }
    }

    def automateScalafmtFor(configurations: Configuration*): Seq[Setting[_]] =
      configurations.flatMap { c =>
        inConfig(c)(
          Seq(
            scalafmtInc := scalafmtIncImpl.value,
            compileInputs.in(compile) := {
              scalafmtInc.value
              compileInputs.in(compile).value
            },
            sourceDirectories.in(scalafmtInc) := Seq(scalaSource.value)
          )
        )
      }
  }

  private val scalafmtInc = taskKey[Unit]("Incrementally format modified sources")

  override def requires = plugins.JvmPlugin

  override def trigger = allRequirements

  override def projectSettings =
    (includeFilter.in(scalafmtInc) := "*.scala") +: autoImport.automateScalafmtFor(Compile, Test)
}