package com.sdg.framework.io

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.fasterxml.jackson.databind.ObjectMapper

/** Orden de campos tal como aparecen en el fichero JSON de entrada.
  *
  * Al leer JSON sin esquema, Spark infiere las columnas en orden alfabetico (`age, name, office`), que no es el orden
  * del fichero (`name, age, office`). Si la fuente declara `preserveFieldOrder`, esta pieza muestrea registros del
  * origen (fichero, directorio o patron), recoge las claves en su orden de primera aparicion y reordena el DataFrame
  * con ese orden. Las columnas no vistas en la muestra (pueden aparecer solo en registros posteriores) se anaden al
  * final en el orden inferido por Spark: ningun campo se pierde.
  *
  * Solo se muestrean unos pocos ficheros/registros: el orden de campos es una propiedad del formato del fichero, no de
  * los datos completos. Si el origen no puede muestrearse (p. ej. JSON multilinea), se conserva el orden inferido por
  * Spark.
  */
object JsonFieldOrder {
  private val log = LoggerFactory.getLogger(getClass)

  private val mapper     = new ObjectMapper()
  private val MaxFiles   = 5
  private val MaxRecords = 200

  def orderedColumns(spark: SparkSession, sourcePath: String, dataFrameColumns: Seq[String]): Seq[String] = {
    val sampled = sampleKeys(spark, sourcePath).filter(dataFrameColumns.contains)
    (sampled ++ dataFrameColumns.filterNot(sampled.contains)).distinct
  }

  private def sampleKeys(spark: SparkSession, sourcePath: String): Seq[String] = {
    val seen = mutable.LinkedHashSet.empty[String]
    try {
      val fs      = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val files   = dataFiles(fs, sourcePath)
      var records = 0
      files.take(MaxFiles).foreach { file =>
        var reader: BufferedReader = null
        try {
          reader = new BufferedReader(new InputStreamReader(fs.open(file), StandardCharsets.UTF_8))
          var line = reader.readLine()
          while (line != null && records < MaxRecords) {
            val trimmed = line.trim
            if (trimmed.nonEmpty) {
              try {
                val node = mapper.readTree(trimmed)
                if (node != null && node.isObject) {
                  node.fieldNames().asScala.foreach(seen += _)
                  records += 1
                } else if (node != null && node.isArray) {
                  node.elements().asScala.filter(_.isObject).foreach { element =>
                    element.fieldNames().asScala.foreach(seen += _)
                    records += 1
                  }
                }
              } catch {
                case NonFatal(_) => // linea no parseable: se ignora en la muestra
              }
            }
            line = reader.readLine()
          }
        } finally {
          if (reader != null) reader.close()
        }
      }
    } catch {
      case NonFatal(error) =>
        log.warn(
          s"No se ha podido muestrear el orden de campos de '$sourcePath' (${error.getMessage}); se usa el orden inferido por Spark"
        )
    }
    seen.toSeq
  }

  private def dataFiles(fs: FileSystem, sourcePath: String): Seq[Path] = {
    val path = new Path(sourcePath)
    val glob = Option(fs.globStatus(path)).map(_.toSeq).getOrElse(Seq.empty)
    val roots =
      if (glob.nonEmpty) glob.map(_.getPath)
      else if (fs.exists(path)) Seq(path)
      else Seq.empty
    roots
      .flatMap { root =>
        val status = fs.getFileStatus(root)
        if (status.isFile) Seq(root)
        else {
          val files    = mutable.ArrayBuffer.empty[Path]
          val iterator = fs.listFiles(root, true)
          while (iterator.hasNext) {
            val located = iterator.next()
            val name    = located.getPath.getName
            if (!name.startsWith("_") && !name.startsWith(".")) files += located.getPath
          }
          files.toSeq
        }
      }
      .sortBy(_.toString)
  }
}
