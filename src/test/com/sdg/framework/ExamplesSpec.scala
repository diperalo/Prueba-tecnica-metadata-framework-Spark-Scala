package com.sdg.framework

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.{Clock, Instant, ZoneId}

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sdg.framework.config.MetadataLoader
import com.sdg.framework.engine.DataflowRunner

/** Verificacion automatica de los dataflows ejemplarizantes de `examples/` (sustituye al antiguo script
  * `verify_examples.py`: la comprobacion vive ahora en la propia bateria de tests, sin dependencias fuera del stack
  * Scala/sbt).
  *
  * Cada ejemplo se ejecuta de forma hermetica: se copia a un directorio temporal reescribiendo los prefijos de ruta de
  * sus metadatos, se lanza con un `Clock` fijo y se comprueba, para cada sink, lo mismo que comprobaba el script:
  *
  *   - el destino contiene UN fichero publicado `<prefijo>_ok|ko_<yyyyMMdd_HHmm>.json`, sin `part-*` ni `_staging` a la
  *     vista del consumidor,
  *   - existe su entrada de catalogo y su contador de registros cuadra con el esperado,
  *   - el contenido coincide con el `expected_*.json` del ejemplo, normalizando `dt` (marca de ejecucion),
  *   - el orden de campos de cada registro es el del fichero esperado.
  */
class ExamplesSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("sdg-metadata-framework-examples-test")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  private val mapper = new ObjectMapper()

  /** Ejemplos a verificar: carpeta en `examples/` y pares (salida relativa, fichero esperado). */
  private val cases: Seq[(String, Seq[(String, String)])] = Seq(
    "01-validaciones-multiples" -> Seq(
      "output/events/employees"   -> "expected/expected_ok.json",
      "output/discards/employees" -> "expected/expected_ko.json"
    ),
    "02-ordenes-csv" -> Seq(
      "output/events/orders"   -> "expected/expected_ok.json",
      "output/discards/orders" -> "expected/expected_ko.json"
    ),
    "03-transformaciones-genericas" -> Seq(
      "output/events/products"   -> "expected/expected_ok.json",
      "output/discards/products" -> "expected/expected_ko.json"
    )
  )

  cases.foreach { case (exampleDir, exampleOutputs) =>
    test(s"el ejemplo $exampleDir publica las salidas esperadas") {
      val sourceDir = Paths.get("examples", exampleDir)
      assert(Files.isDirectory(sourceDir), s"No se encuentra el ejemplo: $sourceDir")

      // Copia hermetica del ejemplo a un directorio temporal
      val tempDir = Files.createTempDirectory("sdg-examples-test")
      copyRecursively(sourceDir.resolve("input"), tempDir.resolve("input"))
      val expectedCopied = tempDir.resolve("expected")
      copyRecursively(sourceDir.resolve("expected"), expectedCopied)

      // Los metadatos apuntan al ejemplo original: se reescribe el prefijo
      // por la ruta absoluta de la copia (con '/', valido en Windows y Linux)
      val metadataText = new String(Files.readAllBytes(sourceDir.resolve("metadata.json")), StandardCharsets.UTF_8)
      val tempPrefix   = tempDir.toString.replace("\\", "/")
      val rewritten    = metadataText.replace(s"examples/$exampleDir", tempPrefix)
      assert(rewritten != metadataText, "Los metadatos no contenian rutas reescribibles")
      val metadataPath = tempDir.resolve("metadata.json")
      Files.write(metadataPath, rewritten.getBytes(StandardCharsets.UTF_8))

      val fixedClock = Clock.fixed(Instant.parse("2026-10-16T10:13:00Z"), ZoneId.of("UTC"))
      new DataflowRunner(spark, fixedClock).run(MetadataLoader.load(metadataPath.toString))

      exampleOutputs.foreach { case (outputRel, expectedRel) =>
        checkOutput(tempDir.resolve(outputRel), expectedCopied.resolve(Paths.get(expectedRel).getFileName))
      }
    }
  }

  // ---------- comprobaciones equivalentes a las del verificador anterior ----------

  private val publishedName = "^[a-z0-9-]+_(ok|ko)_\\d{8}_\\d{4}\\.json$".r

  private def checkOutput(outputDir: Path, expectedFile: Path): Unit = {
    assert(Files.isDirectory(outputDir), s"No existe el directorio de salida: $outputDir")
    val entries = Files.list(outputDir).iterator().asScala.toSeq
    val published =
      entries.filter(p => p.getFileName.toString.endsWith(".json") && !p.getFileName.toString.endsWith(".catalog.json"))
    assert(published.size == 1, s"$outputDir: se esperaba 1 fichero publicado y hay ${published.map(_.getFileName)}")
    val publishedFile = published.head
    assert(
      publishedName.pattern.matcher(publishedFile.getFileName.toString).matches(),
      s"Nombre no conforme al patron: ${publishedFile.getFileName}"
    )
    val leftovers = entries.map(_.getFileName.toString).filter(n => n.contains("part-") || n.contains("_staging"))
    assert(leftovers.isEmpty, s"$outputDir: restos de Spark visibles: $leftovers")

    val expected = normalizedRows(expectedFile)

    val catalogFile =
      outputDir.resolve(publishedFile.getFileName.toString.replaceAll("\\.json$", ".catalog.json"))
    assert(Files.isRegularFile(catalogFile), s"Falta la entrada de catalogo: $catalogFile")
    val catalog = mapper.readTree(catalogFile.toFile)
    assert(
      catalog.get("records").asLong() == expected.size,
      s"$catalogFile: records=${catalog.get("records").asLong()} pero se esperaban ${expected.size}"
    )

    val actual = normalizedRows(publishedFile)
    assert(
      actual.map(_._1) == expected.map(_._1),
      s"Contenido distinto de ${expectedFile.getFileName}\nESPERADO:\n${expected.map(_._1).mkString("\n")}" +
        s"\nACTUAL:\n${actual.map(_._1).mkString("\n")}"
    )
    assert(
      actual.map(_._2) == expected.map(_._2),
      s"Orden de campos distinto del esperado en ${publishedFile.getFileName}: " +
        s"${actual.map(_._2)} frente a ${expected.map(_._2)}"
    )
  }

  /** Filas del fichero como (representacion canonica ordenable, orden de campos), con `dt` normalizado y ordenadas. */
  private def normalizedRows(file: Path): Seq[(String, Seq[String])] =
    Files
      .readAllLines(file)
      .asScala
      .filter(_.trim.nonEmpty)
      .map { line =>
        val row = mapper.readTree(line)
        if (row.has("dt") && row.get("dt").isTextual)
          row.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].put("dt", "<dt>")
        val keys = row.fieldNames().asScala.toSeq
        (canonical(row), keys)
      }
      .toSeq
      .sortBy(_._1)

  /** Representacion canonica de un nodo JSON: objetos con las claves ordenadas, para comparar contenidos sin que el
    * orden de campos influya (ese orden se comprueba aparte).
    */
  private def canonical(node: JsonNode): String =
    if (node.isObject) {
      val fields = node.fields().asScala.toSeq.sortBy(_.getKey)
      fields.map(f => s"${mapper.writeValueAsString(f.getKey)}:${canonical(f.getValue)}").mkString("{", ",", "}")
    } else if (node.isArray) {
      node.elements().asScala.map(n => canonical(n)).mkString("[", ",", "]")
    } else {
      mapper.writeValueAsString(node)
    }

  private def copyRecursively(from: Path, to: Path): Unit = {
    val stream = Files.walk(from)
    try
      stream.forEach { p =>
        val target = to.resolve(from.relativize(p))
        if (Files.isDirectory(p)) Files.createDirectories(target)
        else Files.copy(p, target)
      }
    finally stream.close()
  }
}
