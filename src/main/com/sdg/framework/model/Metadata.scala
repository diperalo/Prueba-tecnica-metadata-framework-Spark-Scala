package com.sdg.framework.model

import com.fasterxml.jackson.databind.JsonNode

/** Modelo de metadatos del framework.
  *
  * Decision de diseno clave: los `params` de una transformacion se conservan como arbol JSON (JsonNode) en lugar de
  * deserializarse a un tipo cerrado. Asi, anadir un nuevo tipo de transformacion NO obliga a modificar este modelo:
  * cada transformacion interpreta sus propios params. El nucleo del framework (DataflowRunner) desconoce los detalles
  * de cada transformacion.
  */
final case class Metadata(dataflows: Seq[Dataflow])

final case class Dataflow(
    name: String,
    sources: Seq[SourceDef],
    transformations: Seq[TransformationDef],
    sinks: Seq[SinkDef]
)

/** Fuente de datos.
  *
  * @param preserveFieldOrder
  *   en JSON, conserva el orden de campos del fichero de entrada (ver JsonFieldOrder) en lugar del orden alfabetico que
  *   infiere Spark. Los campos anadidos despues (dt, etc.) iran a continuacion, en el orden de los metadatos.
  */
final case class SourceDef(
    name: String,
    path: String,
    format: String,
    options: Map[String, String] = Map.empty,
    preserveFieldOrder: Boolean = false
)

final case class TransformationDef(name: String, `type`: String, params: JsonNode)

/** Publicacion de una salida para sus consumidores (ver OutputPublisher).
  *
  * La escritura de Spark es siempre nativa (directorio con `part-*`, sin forzar coalesce). Despues, si el sink declara
  * `publication`, una fase posterior publica UN fichero con el nombre concreto que necesita el consumidor
  * (`<prefijo>_<yyyyMMdd_HHmm>.<ext>`) y lo cataloga con un fichero `<nombre>.catalog.json` a su lado. El procesamiento
  * no sabe nada de ese nombre: es el contrato de entrega, no el calculo.
  *
  * @param filePrefix
  *   override opcional del prefijo; si no se declara se deriva de la fuente + sufijo _ok/_ko del sink.
  * @param catalog
  *   si se escribe el fichero de catalogo junto al publicado.
  */
final case class PublicationDef(filePrefix: Option[String] = None, catalog: Boolean = true)

final case class SinkDef(
    name: String,
    input: String,
    paths: Seq[String],
    format: String,
    saveMode: String = "OVERWRITE",
    partitionBy: Seq[String] = Seq.empty,
    options: Map[String, String] = Map.empty,
    coalesce: Option[Int] = None,
    publication: Option[PublicationDef] = None
)
