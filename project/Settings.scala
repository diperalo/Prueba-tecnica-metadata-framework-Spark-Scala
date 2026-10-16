import sbt.Keys._
import sbt.io.syntax._
import sbt.{Compile, Test, TestFrameworks, Tests}

object Settings {

  // Apertura de modulos necesaria para ejecutar Spark embebido en Java 17+
  // (validas tambien en Java 11). Con spark-submit las aporta el lanzador.
  private val sparkJavaOptions = Seq(
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
  )

  val settings: Seq[sbt.Setting[_]] = Seq(
    name         := "sdg-metadata-framework",
    version      := "1.0.0",
    scalaVersion := "2.12.18",
    organization := "com.sdg",

    // Carpetas sin el subdirectorio por defecto `scala/`:
    // el codigo vive directamente en src/main y src/test por paquete.
    Compile / scalaSource := file((baseDirectory.value / "src" / "main").toString),
    Test / scalaSource    := file((baseDirectory.value / "src" / "test").toString),

    // Opciones del compilador
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-unused"
    ),
    Test / scalacOptions += "-Xcheckinit",

    // Ejecucion y tests
    Test / parallelExecution  := false,
    Test / testForkedParallel := false,
    Test / fork               := true,
    Test / javaOptions ++= sparkJavaOptions ++ Seq("-Xmx512m", "-Dspark.driver.memory=512m"),
    Test / testOptions ++= Seq(
      Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports"),
      Tests.Argument("-oDF")
    ),
    Compile / run / fork        := true,
    Compile / run / javaOptions ++= sparkJavaOptions ++ Seq("-Xmx768m", "-Dspark.driver.memory=512m")
  )
}
