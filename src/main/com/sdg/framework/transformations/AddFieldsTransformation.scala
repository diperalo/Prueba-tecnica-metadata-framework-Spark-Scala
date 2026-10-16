package com.sdg.framework.transformations

import scala.collection.JavaConverters._

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

import com.fasterxml.jackson.databind.JsonNode
import com.sdg.framework.model.TransformationDef

/** Campo derivado declarado en los metadatos. */
final case class AddFieldDef(name: String, function: String, params: Map[String, String] = Map.empty) {
  def value: Option[String]  = params.get("value")
  def format: Option[String] = params.get("format")
  def inputField: Option[String] =
    params.get("inputField").orElse(params.get("field")).orElse(params.get("sourceField"))
}

/** Registro de funciones disponibles para add_fields.
  *
  * EXTENSIBILIDAD: anadir una funcion = registrar una entrada aqui. Si la funcion declara "format", el resultado se
  * formatea con ese patron (p. ej. current_timestamp + "yyyy-MM-dd HH:mm:ss" reproduce el formato del ejemplo
  * STANDARD_OK del enunciado; sin format se conserva el tipo timestamp/date nativo de Spark).
  */
object FieldFunctionRegistry {

  private type Factory = AddFieldDef => Column

  private def requiredField(f: AddFieldDef): String =
    f.inputField.getOrElse(
      throw new IllegalArgumentException(s"La funcion '${f.function}' requiere 'inputField' en el campo '${f.name}'")
    )

  private val factories: Map[String, Factory] = Map(
    "current_timestamp" -> (f =>
      f.format
        .map(fmt => date_format(current_timestamp(), fmt))
        .getOrElse(current_timestamp())
    ),
    "current_date" -> (f =>
      f.format
        .map(fmt => date_format(current_date(), fmt))
        .getOrElse(current_date())
    ),
    "lit"     -> (f => lit(f.value.getOrElse(""))),
    "literal" -> (f => lit(f.value.getOrElse(""))),
    "upper"   -> (f => upper(col(requiredField(f)))),
    "lower"   -> (f => lower(col(requiredField(f)))),
    "trim"    -> (f => trim(col(requiredField(f))))
  )

  def availableFunctions: Seq[String] = factories.keys.toSeq.sorted

  def create(field: AddFieldDef): Column =
    factories.getOrElse(
      field.function.toLowerCase.trim,
      throw new IllegalArgumentException(
        s"Funcion desconocida: '${field.function}' en el campo '${field.name}'. " +
          s"Disponibles: ${availableFunctions.mkString(", ")}"
      )
    )(field)
}

/** add_fields: anade columnas derivadas y produce UNA salida con el nombre exacto de la transformacion (p. ej.
  * "ok_with_date").
  *
  * Ejemplo de params: { "input": "validation_ok", "addFields": [ { "name": "dt", "function": "current_timestamp",
  * "format": "yyyy-MM-dd HH:mm:ss" } ] }
  */
object AddFieldsTransformation extends Transformation {

  override def execute(catalog: Map[String, DataFrame], definition: TransformationDef): Map[String, DataFrame] = {
    val (_, input) = inputDataFrame(catalog, definition)
    val fields     = parseAddFields(definition.params)
    val result = fields.foldLeft(input) { (df, field) =>
      df.withColumn(field.name, FieldFunctionRegistry.create(field))
    }
    Map(definition.name -> result)
  }

  private def parseAddFields(params: JsonNode): Seq[AddFieldDef] = {
    require(
      params.hasNonNull("addFields") && params.get("addFields").isArray,
      "add_fields requiere params.addFields (array)"
    )
    params
      .get("addFields")
      .elements()
      .asScala
      .map { node =>
        require(
          node.hasNonNull("name") && node.hasNonNull("function"),
          s"Cada campo anadido requiere 'name' y 'function': $node"
        )
        val extra = node
          .fields()
          .asScala
          .filterNot(e => Set("name", "function").contains(e.getKey))
          .map(e => e.getKey -> e.getValue.asText())
          .toMap
        AddFieldDef(node.get("name").asText(), node.get("function").asText(), extra)
      }
      .toSeq
  }
}
