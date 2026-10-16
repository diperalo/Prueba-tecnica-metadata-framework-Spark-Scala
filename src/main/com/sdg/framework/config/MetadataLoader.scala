package com.sdg.framework.config

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sdg.framework.model._

/** Carga y valida la estructura minima de los metadatos.
  *
  * Solo valida la "carcasa" comun (dataflows / sources / transformations / sinks). El contenido de `params` lo valida
  * cada transformacion, que es quien conoce su propio contrato. Esto mantiene el loader estable aunque se anadan nuevas
  * transformaciones, validaciones o funciones.
  */
object MetadataLoader {

  private val mapper = new ObjectMapper()

  def load(path: String): Metadata = {
    val content = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
    parse(content)
  }

  def parse(json: String): Metadata = {
    val root = mapper.readTree(json)
    require(
      root != null && root.hasNonNull("dataflows") && root.get("dataflows").isArray,
      "Los metadatos deben contener un array 'dataflows'"
    )

    val dataflows = root.get("dataflows").elements().asScala.map(parseDataflow).toList
    require(dataflows.nonEmpty, "Debe definirse al menos un dataflow")
    Metadata(dataflows)
  }

  /** Expuesto para que las transformaciones creen nodos vacios si falta params. */
  def emptyParams(): JsonNode = mapper.createObjectNode()

  private def parseDataflow(node: JsonNode): Dataflow = {
    Dataflow(
      name = requiredText(node, "name"),
      sources = parseArray(node, "sources").map(parseSource),
      transformations = parseArray(node, "transformations").map(parseTransformation),
      sinks = parseArray(node, "sinks").map(parseSink)
    )
  }

  private def parseSource(node: JsonNode): SourceDef = SourceDef(
    name = requiredText(node, "name"),
    path = requiredText(node, "path"),
    format = textOr(node, "format", "JSON"),
    options = parseOptions(node),
    preserveFieldOrder = node.hasNonNull("preserveFieldOrder") && node.get("preserveFieldOrder").asBoolean(false)
  )

  private def parseTransformation(node: JsonNode): TransformationDef = {
    val params =
      if (node.hasNonNull("params")) node.get("params") else emptyParams()
    TransformationDef(name = requiredText(node, "name"), `type` = requiredText(node, "type"), params = params)
  }

  private def parseSink(node: JsonNode): SinkDef = {
    // Se acepta tanto "paths": [...] (formato del enunciado) como "path": "..."
    val paths =
      if (node.hasNonNull("paths"))
        node.get("paths").elements().asScala.map(_.asText()).toList
      else if (node.hasNonNull("path")) Seq(node.get("path").asText())
      else
        throw new IllegalArgumentException(
          s"El sink '${textOr(node, "name", "?")}' debe definir 'paths' (array) o 'path'"
        )

    SinkDef(
      name = requiredText(node, "name"),
      input = requiredText(node, "input"),
      paths = paths,
      format = textOr(node, "format", "JSON"),
      saveMode = textOr(node, "saveMode", "OVERWRITE"),
      partitionBy =
        if (node.hasNonNull("partitionBy"))
          node.get("partitionBy").elements().asScala.map(_.asText()).toList
        else Seq.empty,
      options = parseOptions(node),
      coalesce = if (node.hasNonNull("coalesce")) Some(node.get("coalesce").asInt()) else None,
      publication = parsePublication(node)
    )
  }

  /** `publication` como objeto; por compatibilidad, el antiguo `singleFileOutput: true` equivale a una publicacion por
    * defecto.
    */
  private def parsePublication(node: JsonNode): Option[PublicationDef] =
    if (node.hasNonNull("publication") && node.get("publication").isObject) {
      val p = node.get("publication")
      Some(
        PublicationDef(
          filePrefix = if (p.hasNonNull("filePrefix")) Some(p.get("filePrefix").asText()) else None,
          catalog = !p.hasNonNull("catalog") || p.get("catalog").asBoolean(true)
        )
      )
    } else if (node.hasNonNull("singleFileOutput") && node.get("singleFileOutput").asBoolean(false)) {
      Some(PublicationDef())
    } else None

  // ---- helpers ----

  // List (estricta), nunca Stream: los iteradores de Jackson darian
  // colecciones perezosas cuyo recorrido parcial saltaria elementos.
  private def parseArray(node: JsonNode, field: String): Seq[JsonNode] =
    if (node.hasNonNull(field) && node.get(field).isArray)
      node.get(field).elements().asScala.toList
    else List.empty

  private def parseOptions(node: JsonNode): Map[String, String] =
    if (node.hasNonNull("options") && node.get("options").isObject) {
      node
        .get("options")
        .fields()
        .asScala
        .map { e =>
          e.getKey -> e.getValue.asText()
        }
        .toMap
    } else Map.empty

  private def requiredText(node: JsonNode, field: String): String = {
    require(
      node.hasNonNull(field) && node.get(field).asText().trim.nonEmpty,
      s"Falta el campo obligatorio '$field' en: $node"
    )
    node.get(field).asText()
  }

  private def textOr(node: JsonNode, field: String, default: String): String =
    if (node.hasNonNull(field)) node.get(field).asText() else default
}
