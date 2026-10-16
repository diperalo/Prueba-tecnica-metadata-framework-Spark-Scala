package com.sdg.framework.transformations

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import com.fasterxml.jackson.databind.JsonNode
import com.sdg.framework.model.TransformationDef
import com.sdg.framework.validation.{FieldValidation, ValidationInstance, ValidationRegistry}

/** validate_fields: aplica las validaciones declaradas campo a campo y parte el dataset en dos salidas con nombre
  * derivado del de la transformacion: <name>_ok y <name>_ko.
  *
  *   - _ok : registros que superan TODAS las validaciones, sin columnas extra.
  *   - _ko : registros con al menos un fallo, con una columna adicional `arraycoderrorbyfield`: map<campo,
  *     array<codigos de error>> solo con los campos que fallan, para que el descarte sea autoexplicativo sin necesidad
  *     de reejecutar el proceso.
  *
  * Un campo ausente en el esquema se materializa como null antes de validar (p. ej. `age` en un JSON sin esa clave), de
  * modo que el registro KO conserva el campo y la validacion notNull puede detectarlo.
  *
  * Ejemplo de params: { "input": "person_inputs", "validations": [ { "field": "office", "validations": ["notEmpty"] },
  * { "field": "age", "validations": ["notNull"] } ] }
  */
object ValidateFieldsTransformation extends Transformation {

  private val ErrorMapColumn = "arraycoderrorbyfield"

  override def execute(catalog: Map[String, DataFrame], definition: TransformationDef): Map[String, DataFrame] = {

    val (_, input)  = inputDataFrame(catalog, definition)
    val validations = parseValidations(definition.params)

    // 1) Materializar como null los campos validados que no existan.
    val normalized = validations.map(_.field).distinct.foldLeft(input) { (df, field) =>
      if (df.columns.contains(field)) df else df.withColumn(field, lit(null))
    }

    // 2) Una columna temporal por campo con los codigos que incumple.
    val indexed = validations.zipWithIndex
    val withErrors = indexed.foldLeft(normalized) { case (df, (fv, idx)) =>
      df.withColumn(tempColumn(idx), errorCodesFor(fv, col(fv.field)))
    }
    val tempColumns = indexed.map { case (_, idx) => tempColumn(idx) }

    // 3) Mapa campo -> codigos, solo con campos que fallan (map_filter).
    val errorMap = map(indexed.flatMap { case (fv, idx) =>
      Seq(lit(fv.field), col(tempColumn(idx)))
    }: _*)
    val withErrorMap = withErrors.withColumn("__error_map", errorMap)
    val hasError = tempColumns
      .map(name => size(col(name)) > 0)
      .reduceOption(_ || _)
      .getOrElse(lit(false))

    val ok = withErrorMap
      .filter(!hasError)
      .drop(tempColumns ++ Seq("__error_map"): _*)

    val ko = withErrorMap
      .filter(hasError)
      .withColumn(ErrorMapColumn, expr("map_filter(__error_map, (k, v) -> size(v) > 0)"))
      .drop(tempColumns ++ Seq("__error_map"): _*)

    Map(s"${definition.name}_ok" -> ok, s"${definition.name}_ko" -> ko)
  }

  /** Array con un codigo por cada validacion incumplida (vacio si pasa). */
  private def errorCodesFor(fv: FieldValidation, value: org.apache.spark.sql.Column) = {
    val empty = lit(Array.empty[String])
    fv.validations
      .map(ValidationRegistry.create)
      .foldLeft(empty) { (acc, rule) =>
        concat(acc, when(rule.isInvalid(value), array(lit(rule.errorCode))).otherwise(empty))
      }
  }

  private def tempColumn(idx: Int): String = s"__errors_$idx"

  // ---- Parseo de params (forma simple y forma con parametros) ----

  private def parseValidations(params: JsonNode): Seq[FieldValidation] = {
    require(
      params.hasNonNull("validations") && params.get("validations").isArray,
      "validate_fields requiere params.validations (array)"
    )
    params
      .get("validations")
      .elements()
      .asScala
      .map { node =>
        require(node.hasNonNull("field"), s"Cada validacion debe definir 'field': $node")
        val rules = Option(node.get("validations"))
          .filter(_.isArray)
          .map(_.elements().asScala.map(parseRule).toSeq)
          .getOrElse(Seq.empty)
        FieldValidation(node.get("field").asText(), rules)
      }
      .toSeq
  }

  private def parseRule(node: JsonNode): ValidationInstance =
    if (node.isTextual) ValidationInstance(node.asText())
    else if (node.isObject) {
      val ruleName =
        if (node.hasNonNull("rule")) node.get("rule").asText()
        else if (node.hasNonNull("name")) node.get("name").asText()
        else if (node.hasNonNull("type")) node.get("type").asText()
        else throw new IllegalArgumentException(s"Validacion sin 'rule': $node")
      val params = node
        .fields()
        .asScala
        .filterNot(e => Set("rule", "name", "type").contains(e.getKey))
        .map(e => e.getKey -> e.getValue.asText())
        .toMap
      ValidationInstance(ruleName, params)
    } else throw new IllegalArgumentException(s"Validacion no soportada: $node")
}
