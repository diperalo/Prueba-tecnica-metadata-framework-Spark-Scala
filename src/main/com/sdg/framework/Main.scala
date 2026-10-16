package com.sdg.framework

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.sdg.framework.config.MetadataLoader
import com.sdg.framework.engine.DataflowRunner

/** Punto de entrada.
  *
  * Uso: sbt "run config/metadata.json" sbt "run config/metadata.json --dataflow=prueba-acceso" spark-submit --master
  * local[*] --class com.sdg.framework.Main \ target/scala-2.12/sdg-metadata-framework-assembly-1.0.0.jar
  * config/metadata.json
  *
  * El proceso es dirigido por los metadatos indicados: el mismo binario ejecuta cualquier dataflow descrito en el
  * fichero, sin cambios de codigo.
  */
object Main {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.isEmpty || args.contains("--help") || args.contains("-h")) {
      println(
        """Uso: Main <ruta-metadatos.json> [--dataflow=<nombre>]
          |  Ejecuta los dataflows descritos en el fichero de metadatos.
          |  Sin --dataflow se ejecutan todos en orden.""".stripMargin
      )
      if (args.isEmpty) System.exit(1) else return
    }

    val metadataPath = args
      .find(a => !a.startsWith("--"))
      .getOrElse(throw new IllegalArgumentException("Debe indicarse la ruta del fichero de metadatos"))
    val onlyDataflow = args.collectFirst {
      case a if a.startsWith("--dataflow=") => a.stripPrefix("--dataflow=")
    }

    // En spark-submit el master lo fija el comando; en ejecucion local
    // (sbt run) se usa local[*] si nadie ha fijado spark.master.
    val builder = SparkSession.builder().appName("sdg-metadata-framework")
    if (sys.props.get("spark.master").isEmpty && sys.env.get("SPARK_MASTER").isEmpty)
      builder.master("local[*]")

    val spark = builder.getOrCreate()
    try {
      log.info(s"Cargando metadatos desde '$metadataPath'")
      val metadata = MetadataLoader.load(metadataPath)
      new DataflowRunner(spark).run(metadata, onlyDataflow)
    } finally {
      spark.stop()
    }
  }
}
