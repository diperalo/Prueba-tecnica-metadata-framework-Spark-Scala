import sbt._

object Dependencies {
  val sparkVersion    = "3.5.1"
  val jacksonVersion  = "2.15.2"
  val scalaTestVersion = "3.2.18"

  // Ambito Compile (no Provided) a proposito: en este ejercicio Lite se
  // ejecuta con `sbt run` sin cluster Spark. En un despliegue real con
  // spark-submit se cambiarian spark-core/spark-sql a % "provided" y el
  // cluster aportaria Spark.
  private val prod = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql"  % sparkVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
  )

  private val test = Seq(
    "org.scalatest" %% "scalatest" % scalaTestVersion
  ).map(_ % Test)

  val all: Seq[ModuleID] = prod ++ test
}
