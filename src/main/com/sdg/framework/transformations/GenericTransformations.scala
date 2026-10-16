package com.sdg.framework.transformations

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

import com.sdg.framework.model.TransformationDef

/** Transformaciones genericas adicionales. No las pide el enunciado: se incluyen para demostrar que el motor no es un
  * proceso ad-hoc de "person", sino un framework al que se conectan piezas por tipo. Todas producen una salida con el
  * nombre exacto de la transformacion.
  */

/** rename_fields: { "input": "x", "renames": [{ "from": "a", "to": "b" }] } */
object RenameFieldsTransformation extends Transformation {
  override def execute(catalog: Map[String, DataFrame], definition: TransformationDef): Map[String, DataFrame] = {
    val (_, input) = inputDataFrame(catalog, definition)
    val renames = definition.params
      .get("renames")
      .elements()
      .asScala
      .map { n =>
        n.get("from").asText() -> n.get("to").asText()
      }
      .toSeq
    val result = renames.foldLeft(input) { case (df, (from, to)) =>
      if (df.columns.contains(from)) df.withColumnRenamed(from, to) else df
    }
    Map(definition.name -> result)
  }
}

/** drop_fields: { "input": "x", "fields": ["a", "b"] } */
object DropFieldsTransformation extends Transformation {
  override def execute(catalog: Map[String, DataFrame], definition: TransformationDef): Map[String, DataFrame] = {
    val (_, input) = inputDataFrame(catalog, definition)
    val fields = definition.params
      .get("fields")
      .elements()
      .asScala
      .map(_.asText())
      .toSeq
    Map(definition.name -> input.drop(fields: _*))
  }
}

/** filter: { "input": "x", "condition": "age >= 18" } (expresion SQL Spark) */
object FilterTransformation extends Transformation {
  override def execute(catalog: Map[String, DataFrame], definition: TransformationDef): Map[String, DataFrame] = {
    val (_, input) = inputDataFrame(catalog, definition)
    val condition  = definition.params.get("condition").asText()
    Map(definition.name -> input.filter(expr(condition)))
  }
}
