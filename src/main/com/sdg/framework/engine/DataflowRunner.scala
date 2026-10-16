package com.sdg.framework.engine

import java.time.format.DateTimeFormatter
import java.time.{Clock, LocalDateTime}

import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import com.sdg.framework.io.{OutputCatalog, OutputFileNaming, OutputNamingContext, OutputPublisher, SinkWriter, SourceReader}
import com.sdg.framework.model.{Dataflow, Metadata}
import com.sdg.framework.transformations.TransformationRegistry

/** Motor de ejecucion: interpreta los metadatos de forma dinamica.
  *
  * Flujo por dataflow (sin ninguna logica de negocio codificada):
  *   1. Carga cada source y lo registra en el catalogo con su nombre logico.
  *   2. Ejecuta las transformations EN ORDEN. Cada una resuelve su input por nombre desde el catalogo y registra sus
  *      salidas (tambien por nombre). Por eso un paso puede consumir "validation_ok" y el siguiente producir
  *      "ok_with_date", tal como describe el enunciado.
  *   3. Escribe cada sink (Spark nativo) resolviendo su input por nombre.
  *   4. Publica las salidas que lo declaren (OutputPublisher): el fichero con el nombre concreto que espera el
  *      consumidor, y lo cataloga (OutputCatalog). Es una fase de entrega posterior y separada de la escritura: el
  *      procesamiento no conoce esos nombres.
  *
  * El runner no sabe que es una persona, ni que campos se validan, ni que columna se anade: cambiar los metadatos
  * cambia el proceso sin recompilar.
  */
class DataflowRunner(spark: SparkSession, clock: Clock = Clock.systemDefaultZone()) {

  private val log = LoggerFactory.getLogger(getClass)

  // Formato de la marca de ejecucion usada en los nombres de fichero unico.
  private val ExecutionTimestampFormat = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm")

  def run(metadata: Metadata, onlyDataflow: Option[String] = None): Unit = {
    val selected = onlyDataflow match {
      case Some(name) =>
        val found = metadata.dataflows.filter(_.name == name)
        require(
          found.nonEmpty,
          s"No existe el dataflow '$name'. Disponibles: ${metadata.dataflows.map(_.name).mkString(", ")}"
        )
        found
      case None => metadata.dataflows
    }
    selected.foreach(runDataflow)
  }

  private def runDataflow(dataflow: Dataflow): Unit = {
    log.info(s"=== Ejecutando dataflow '${dataflow.name}' ===")
    val catalog = mutable.LinkedHashMap.empty[String, DataFrame]

    dataflow.sources.foreach { source =>
      catalog += source.name -> SourceReader.read(spark, source)
    }

    dataflow.transformations.foreach { definition =>
      val transformation = TransformationRegistry.get(definition)
      val outputs        = transformation.execute(catalog.toMap, definition)
      outputs.foreach { case (name, df) =>
        log.info(
          s"Transformacion '${definition.name}' (${definition.`type`}) -> dataset '$name' [${df.schema.simpleString}]"
        )
        catalog += name -> df
      }
    }

    // Marca de ejecucion unica para todo el dataflow: OK y KO comparten sufijo.
    val naming = OutputNamingContext(
      sourceBaseName = dataflow.sources.headOption
        .map(source => OutputFileNaming.sourceBaseName(source.path))
        .getOrElse("output"),
      executionTimestamp = LocalDateTime.now(clock).format(ExecutionTimestampFormat)
    )

    val written = dataflow.sinks.flatMap { sink =>
      val df = catalog.getOrElse(
        sink.input,
        throw new IllegalArgumentException(
          s"El sink '${sink.name}' referencia un input inexistente: '${sink.input}'. " +
            s"Disponibles: ${catalog.keys.mkString(", ")}"
        )
      )
      SinkWriter.write(df, sink, dataflow.name)
    }

    // Fases posteriores de entrega: publicacion y catalogacion.
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val published  = OutputPublisher.publish(written, naming, hadoopConf)
    OutputCatalog.write(published, hadoopConf, clock)

    log.info(s"=== Dataflow '${dataflow.name}' finalizado ===")
  }
}
