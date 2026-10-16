package com.sdg.framework.transformations

import org.apache.spark.sql.DataFrame

import com.sdg.framework.model.TransformationDef

/** Contrato de toda transformacion del framework.
  *
  * Una transformacion recibe el catalogo completo de datasets disponibles (nombre logico -> DataFrame) y devuelve los
  * datasets NUEVOS que produce (nombre logico -> DataFrame). El DataflowRunner los registra en el catalogo y las
  * siguientes transformaciones / sinks pueden referenciarlos por nombre. Nadie conoce nombres de ficheros ni pasos
  * concretos salvo lo que digan los metadatos.
  */
trait Transformation {

  /** Ejecuta la transformacion y devuelve sus salidas con nombre logico. */
  def execute(catalog: Map[String, DataFrame], definition: TransformationDef): Map[String, DataFrame]

  /** Resuelve el dataset de entrada declarado en params.input. */
  protected def inputDataFrame(catalog: Map[String, DataFrame], definition: TransformationDef): (String, DataFrame) = {
    val params = definition.params
    require(
      params.hasNonNull("input"),
      s"La transformacion '${definition.name}' (${definition.`type`}) debe definir params.input"
    )
    val inputName = params.get("input").asText()
    val df = catalog.getOrElse(
      inputName,
      throw new IllegalArgumentException(
        s"La transformacion '${definition.name}' referencia un input inexistente: " +
          s"'$inputName'. Disponibles: ${catalog.keys.mkString(", ")}"
      )
    )
    (inputName, df)
  }
}

/** Registro tipo -> transformacion.
  *
  * EXTENSIBILIDAD: anadir una transformacion = implementar Transformation y registrarla aqui (o con `register` en
  * tiempo de arranque). Ni el runner ni los metadatos existentes cambian.
  */
object TransformationRegistry {

  private var registry: Map[String, Transformation] = Map(
    "validate_fields" -> ValidateFieldsTransformation,
    "add_fields"      -> AddFieldsTransformation,
    "rename_fields"   -> RenameFieldsTransformation,
    "drop_fields"     -> DropFieldsTransformation,
    "filter"          -> FilterTransformation
  )

  def register(`type`: String, transformation: Transformation): Unit =
    registry = registry + (normalize(`type`) -> transformation)

  def availableTypes: Seq[String] = registry.keys.toSeq.sorted

  def get(definition: TransformationDef): Transformation =
    registry.getOrElse(
      normalize(definition.`type`),
      throw new IllegalArgumentException(
        s"Tipo de transformacion desconocido: '${definition.`type`}' " +
          s"en '${definition.name}'. Disponibles: ${availableTypes.mkString(", ")}"
      )
    )

  private def normalize(t: String): String = t.toLowerCase.trim
}
