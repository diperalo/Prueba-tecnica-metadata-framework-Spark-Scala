package com.sdg.framework.validation

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/** Una validacion declarada en los metadatos.
  *
  * Ademas de la forma simple del enunciado ("notEmpty"), se acepta la forma con parametros, sin cambiar el formato para
  * quien no los necesite: "validations": ["notNull"] "validations": [{ "rule": "minValue", "value": "18" }]
  */
final case class ValidationInstance(rule: String, params: Map[String, String] = Map.empty)

/** Validaciones de un campo, tal como aparecen en los metadatos. */
final case class FieldValidation(field: String, validations: Seq[ValidationInstance])

/** Regla de validacion reutilizable.
  *
  * `isInvalid` devuelve una Column booleana: true cuando el valor NO supera la validacion. `errorCode` es el codigo
  * autoexplicativo que se escribira en `arraycoderrorbyfield` del fichero STANDARD_KO.
  */
trait ValidationRule {
  def errorCode: String
  def isInvalid(value: Column): Column
}

/** Registro de reglas disponibles.
  *
  * EXTENSIBILIDAD: anadir una validacion = registrar una entrada aqui. Ni el runner ni la transformacion
  * validate_fields cambian. Las claves se normalizan (minusculas, sin '_' ni '-') para que en los metadatos "notNull",
  * "not_null" y "NOTNULL" sean equivalentes.
  */
object ValidationRegistry {

  private def notNullRule = new ValidationRule {
    val errorCode                        = "NULL_NOT_ALLOWED"
    def isInvalid(value: Column): Column = value.isNull
  }

  private def notEmptyRule = new ValidationRule {
    val errorCode = "EMPTY_NOT_ALLOWED"
    def isInvalid(value: Column): Column =
      value.isNull || (trim(value.cast("string")) === "")
  }

  private def positiveRule = new ValidationRule {
    val errorCode = "NOT_POSITIVE"
    def isInvalid(value: Column): Column =
      value.isNull || (value.cast("double") <= 0.0)
  }

  private def nonNegativeRule = new ValidationRule {
    val errorCode = "NEGATIVE_NOT_ALLOWED"
    def isInvalid(value: Column): Column =
      value.isNull || (value.cast("double") < 0.0)
  }

  private def minValueRule(min: String) = new ValidationRule {
    val errorCode = "BELOW_MIN_VALUE"
    def isInvalid(value: Column): Column =
      value.isNull || (value.cast("double") < min.toDouble)
  }

  private def maxValueRule(max: String) = new ValidationRule {
    val errorCode = "ABOVE_MAX_VALUE"
    def isInvalid(value: Column): Column =
      value.isNull || (value.cast("double") > max.toDouble)
  }

  private def allowedValuesRule(values: Seq[String]) = new ValidationRule {
    val errorCode = "VALUE_NOT_IN_ALLOWED_SET"
    def isInvalid(value: Column): Column =
      value.isNull || !value.isin(values: _*)
  }

  private def regexRule(pattern: String) = new ValidationRule {
    val errorCode = "VALUE_DOES_NOT_MATCH_PATTERN"
    def isInvalid(value: Column): Column =
      value.isNull || !value.cast("string").rlike(pattern)
  }

  private def minLengthRule(n: String) = new ValidationRule {
    val errorCode = "VALUE_TOO_SHORT"
    def isInvalid(value: Column): Column =
      value.isNull || (length(value.cast("string")) < n.toInt)
  }

  private def maxLengthRule(n: String) = new ValidationRule {
    val errorCode = "VALUE_TOO_LONG"
    def isInvalid(value: Column): Column =
      value.isNull || (length(value.cast("string")) > n.toInt)
  }

  private type Factory = Map[String, String] => ValidationRule

  private val factories: Map[String, Factory] = Map(
    "notnull"       -> (_ => notNullRule),
    "notempty"      -> (_ => notEmptyRule),
    "notblank"      -> (_ => notEmptyRule),
    "positive"      -> (_ => positiveRule),
    "nonnegative"   -> (_ => nonNegativeRule),
    "minvalue"      -> (p => minValueRule(required(p, "value", "minValue"))),
    "maxvalue"      -> (p => maxValueRule(required(p, "value", "maxValue"))),
    "inset"         -> (p => allowedValuesRule(splitValues(p))),
    "allowedvalues" -> (p => allowedValuesRule(splitValues(p))),
    "matchesregex"  -> (p => regexRule(required(p, "pattern", "matchesRegex"))),
    "regex"         -> (p => regexRule(required(p, "pattern", "regex"))),
    "minlength"     -> (p => minLengthRule(required(p, "value", "minLength"))),
    "maxlength"     -> (p => maxLengthRule(required(p, "value", "maxLength")))
  )

  def availableRules: Seq[String] = factories.keys.toSeq.sorted

  def create(instance: ValidationInstance): ValidationRule = {
    val key = normalize(instance.rule)
    factories.get(key) match {
      case Some(factory) => factory(instance.params)
      case None =>
        throw new IllegalArgumentException(
          s"Validacion desconocida: '${instance.rule}'. " +
            s"Disponibles: ${availableRules.mkString(", ")}"
        )
    }
  }

  private def normalize(rule: String): String =
    rule.toLowerCase.replace("_", "").replace("-", "").replace(" ", "")

  private def required(params: Map[String, String], key: String, rule: String): String =
    params.getOrElse(key, throw new IllegalArgumentException(s"La validacion '$rule' requiere el parametro '$key'"))

  private def splitValues(params: Map[String, String]): Seq[String] = {
    val raw = params
      .get("values")
      .orElse(params.get("value"))
      .getOrElse(
        throw new IllegalArgumentException("La validacion 'inSet' requiere el parametro 'values' (separado por comas)")
      )
    raw.split(",").map(_.trim).filter(_.nonEmpty).toSeq
  }
}
