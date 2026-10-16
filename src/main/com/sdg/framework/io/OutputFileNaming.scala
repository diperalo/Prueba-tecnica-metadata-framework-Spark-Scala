package com.sdg.framework.io

import com.sdg.framework.model.SinkDef

/** Contexto de nombrado compartido por todos los sinks de un dataflow.
  *
  * La marca de ejecucion se calcula UNA sola vez por dataflow (mismo reloj inyectado en el runner), de modo que los
  * ficheros OK y KO de una misma ejecucion comparten siempre el mismo sufijo temporal.
  */
final case class OutputNamingContext(sourceBaseName: String, executionTimestamp: String)

/** Reglas de nombre para los ficheros publicados (`publication` en el sink).
  *
  * Formato: `<prefijo>_<yyyyMMdd_HHmm>.<extension segun format>`
  *
  * Ejemplos (fuente `person.json`, ejecucion 2026-10-16 10:13):
  *   - `person_ok_20261016_1013.json`
  *   - `person_ko_20261016_1013.json`
  *
  * El prefijo se deriva, no se codifica:
  *   1. Si la publicacion declara `filePrefix`, se usa tal cual (override).
  *   2. En caso contrario: nombre base de la fuente (sin ruta, sin extension, y sin el asterisco final si la ruta es un
  *      patron) + sufijo `_ok` /`_ko` deducido de los tokens del nombre del sink o de su dataset de entrada (p. ej.
  *      `raw-ok` / `validation_ko` -> `ko`).
  */
object OutputFileNaming {

  private val KnownExtensions = Set("json", "csv", "parquet", "avro", "orc", "txt", "text")

  /** Nombre base a partir de la ruta de la fuente: el ultimo segmento, sin extension conocida. Sirve igual para rutas a
    * fichero, a directorio o terminadas en asterisco (patron).
    */
  def sourceBaseName(sourcePath: String): String = {
    val cleaned     = sourcePath.trim.stripSuffix("/*").stripSuffix("/").stripSuffix("/*")
    val lastSegment = cleaned.split("/").filter(_.nonEmpty).lastOption.getOrElse("output")
    val withoutExtension = lastSegment.lastIndexOf('.') match {
      case idx if idx > 0 && KnownExtensions.contains(lastSegment.substring(idx + 1).toLowerCase) =>
        lastSegment.substring(0, idx)
      case _ => lastSegment
    }
    sanitize(withoutExtension)
  }

  /** Sufijo de estado deducido del sink: `ok`, `ko` o ninguno. */
  def statusSuffix(sink: SinkDef): Option[String] = {
    val tokens = s"${sink.name} ${sink.input}".toLowerCase.split("[^a-z0-9]+").toSet
    if (tokens.contains("ko")) Some("ko")
    else if (tokens.contains("ok")) Some("ok")
    else None
  }

  def prefixFor(sink: SinkDef, sourceBaseName: String): String =
    sink.publication.flatMap(_.filePrefix).map(sanitize).getOrElse {
      statusSuffix(sink) match {
        case Some(status) => s"${sourceBaseName}_$status"
        case None         => sourceBaseName
      }
    }

  def extensionFor(format: String): String =
    format.toLowerCase match {
      case "text" => "txt"
      case other  => other
    }

  def fileNameFor(sink: SinkDef, context: OutputNamingContext): String =
    s"${prefixFor(sink, context.sourceBaseName)}_${context.executionTimestamp}.${extensionFor(sink.format)}"

  private def sanitize(value: String): String =
    value.replaceAll("[^A-Za-z0-9_-]", "_")
}
