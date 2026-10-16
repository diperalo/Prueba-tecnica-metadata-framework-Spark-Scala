package com.sdg.framework.io

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Clock, LocalDateTime}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import com.fasterxml.jackson.databind.ObjectMapper

/** Fichero ya publicado para sus consumidores, listo para catalogar. */
final case class PublishedFile(
    dataflow: String,
    sink: String,
    inputDataset: String,
    status: Option[String],
    fileName: String,
    destinationDir: String,
    path: String,
    format: String,
    executionTimestamp: String,
    records: Long,
    bytes: Long,
    schema: String,
    catalogEnabled: Boolean
)

/** Fase de PUBLICACION de salidas (posterior a la escritura de Spark).
  *
  * El consumidor de estos datos no consume directorios `part-*` de Spark: necesita UN fichero con un nombre concreto,
  *   p. ej. `person_ok_20261016_1013.json`. Esa necesidad es un contrato de entrega, no parte del procesamiento, por
  *      eso vive aqui y no en el writer ni en las transformaciones:
  *
  *   1. Spark ya ha escrito el dataset de forma nativa y distribuida en el area de staging (varios `part-*` en
  *      paralelo, sin coalesce).
  *   2. Esta fase compone el fichero del consumidor concatenando los `part-*` (formatos de texto orientados a lineas:
  *      JSON, CSV, TEXT) en el destino con el nombre de OutputFileNaming, contando registros y bytes por el camino.
  *   3. El area de staging se elimina: al consumidor solo le llega el fichero publicado y su entrada de catalogo
  *      (OutputCatalog).
  *
  * Si un consumidor aceptara el directorio nativo, bastaria no declarar `publication` en su sink y esta fase no
  * intervendria.
  */
object OutputPublisher {
  private val log = LoggerFactory.getLogger(getClass)

  private val LineOrientedFormats = Set("json", "csv", "text", "txt")

  def publish(
      written: Seq[WrittenOutput],
      naming: OutputNamingContext,
      hadoopConf: Configuration
  ): Seq[PublishedFile] = {
    val fs = FileSystem.get(hadoopConf)
    written.filter(_.stagedForPublication).toList.map { output =>
      val sink = output.sink
      require(
        LineOrientedFormats.contains(sink.format.toLowerCase),
        s"El sink '${sink.name}' pide publication con formato '${sink.format}', que no es orientado a lineas. " +
          s"Formatos publicables como fichero unico: ${LineOrientedFormats.mkString(", ")}"
      )
      val fileName    = OutputFileNaming.fileNameFor(sink, naming)
      val destination = new Path(output.destinationDir, fileName)
      val staging     = new Path(output.writtenPath)
      log.info(s"Publicando sink '${sink.name}' como '$destination' para sus consumidores")
      try {
        val partFiles = fs
          .listStatus(staging)
          .map(_.getPath)
          .filter(p => p.getName.startsWith("part-") && !p.getName.endsWith(".crc"))
          .sortBy(_.getName)
          .toSeq
        require(partFiles.nonEmpty, s"No se han encontrado ficheros part-* en el staging '$staging'")

        fs.mkdirs(new Path(output.destinationDir))
        var records           = 0L
        var bytes             = 0L
        var out: OutputStream = null
        try {
          out = fs.create(destination, true)
          val buffer = new Array[Byte](8192)
          partFiles.foreach { part =>
            var in: InputStream = null
            try {
              in = fs.open(part)
              var read = in.read(buffer)
              while (read >= 0) {
                if (read > 0) {
                  out.write(buffer, 0, read)
                  bytes += read
                  var i = 0
                  while (i < read) {
                    if (buffer(i) == '\n') records += 1
                    i += 1
                  }
                }
                read = in.read(buffer)
              }
            } finally {
              if (in != null) in.close()
            }
          }
        } finally {
          if (out != null) out.close()
        }
        HadoopSidecars.delete(fs, new Path(output.destinationDir, s".$fileName.crc"))

        PublishedFile(
          dataflow = output.dataflowName,
          sink = sink.name,
          inputDataset = sink.input,
          status = OutputFileNaming.statusSuffix(sink),
          fileName = fileName,
          destinationDir = output.destinationDir,
          path = destination.toString,
          format = sink.format.toLowerCase,
          executionTimestamp = naming.executionTimestamp,
          records = records,
          bytes = bytes,
          schema = output.schema.simpleString,
          catalogEnabled = sink.publication.exists(_.catalog)
        )
      } finally {
        if (fs.exists(staging)) fs.delete(staging, true)
        // Limpia el directorio _staging si ha quedado vacio
        val stagingParent = staging.getParent
        if (fs.exists(stagingParent) && fs.listStatus(stagingParent).isEmpty) fs.delete(stagingParent, false)
      }
    }
  }
}

private[io] object HadoopSidecars {

  /** El FileSystem local de Hadoop deja un `.<fichero>.crc` oculto junto a cada fichero creado: se retira para que el
    * consumidor solo vea el fichero publicado y su catalogo.
    */
  def delete(fs: FileSystem, sidecar: Path): Unit =
    if (fs.exists(sidecar)) fs.delete(sidecar, false)
}

/** Fase de CATALOGACION: junto a cada fichero publicado escribe su entrada de catalogo
  * `<nombre-sin-extension>.catalog.json`, para que cualquier consumidor pueda descubrir que se publico, cuando, desde
  * que dataset, con que esquema y cuantos registros, sin abrir el fichero de datos.
  *
  * Ejemplo (person_ok_20261016_1013.catalog.json): { "dataflow": "prueba-acceso", "sink": "raw-ok", "status": "ok",
  * "fileName": "person_ok_20261016_1013.json", "records": 1, ... }
  */
object OutputCatalog {
  private val log = LoggerFactory.getLogger(getClass)

  private val mapper = new ObjectMapper()

  def write(published: Seq[PublishedFile], hadoopConf: Configuration, clock: Clock): Seq[String] = {
    val fs = FileSystem.get(hadoopConf)
    published.filter(_.catalogEnabled).toList.map { file =>
      val catalogName = file.fileName.replaceAll("\\.[^.]+$", "") + ".catalog.json"
      val catalogPath = new Path(file.destinationDir, catalogName)

      val entry = mapper.createObjectNode()
      entry.put("dataflow", file.dataflow)
      entry.put("sink", file.sink)
      entry.put("inputDataset", file.inputDataset)
      file.status.foreach(s => entry.put("status", s))
      entry.put("fileName", file.fileName)
      entry.put("path", file.path)
      entry.put("format", file.format)
      entry.put("executionTimestamp", file.executionTimestamp)
      entry.put("publishedAt", LocalDateTime.now(clock).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
      entry.put("records", file.records)
      entry.put("bytes", file.bytes)
      entry.put("schema", file.schema)

      val content           = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(entry)
      var out: OutputStream = null
      try {
        out = fs.create(catalogPath, true)
        out.write(content.getBytes(StandardCharsets.UTF_8))
        out.write('\n')
      } finally {
        if (out != null) out.close()
      }
      HadoopSidecars.delete(fs, new Path(file.destinationDir, s".$catalogName.crc"))
      log.info(s"Fichero '${file.fileName}' catalogado en '$catalogPath' (${file.records} registros)")
      catalogPath.toString
    }
  }
}
