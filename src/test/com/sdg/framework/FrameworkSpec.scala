package com.sdg.framework

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.{Clock, Instant, ZoneId}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.sdg.framework.config.MetadataLoader
import com.sdg.framework.engine.DataflowRunner
import com.sdg.framework.io.{OutputCatalog, OutputFileNaming, OutputNamingContext, OutputPublisher, SinkWriter}
import com.sdg.framework.model.{PublicationDef, SinkDef}
import com.sdg.framework.transformations.{AddFieldDef, FieldFunctionRegistry}
import com.sdg.framework.validation.{ValidationInstance, ValidationRegistry}

class FrameworkSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("sdg-metadata-framework-test")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  // ---------- Validaciones (unidad) ----------

  test("las reglas notNull y notEmpty generan codigos autoexplicativos") {
    assert(ValidationRegistry.create(ValidationInstance("notNull")).errorCode == "NULL_NOT_ALLOWED")
    assert(ValidationRegistry.create(ValidationInstance("not_empty")).errorCode == "EMPTY_NOT_ALLOWED")
  }

  test("una validacion desconocida falla indicando las disponibles") {
    val ex = intercept[IllegalArgumentException] {
      ValidationRegistry.create(ValidationInstance("noExiste"))
    }
    assert(ex.getMessage.contains("Disponibles"))
  }

  test("las validaciones parametrizadas se crean desde los metadatos") {
    val rule = ValidationRegistry.create(ValidationInstance("minValue", Map("value" -> "18")))
    assert(rule.errorCode == "BELOW_MIN_VALUE")
  }

  test("add_fields rechaza funciones desconocidas y acepta current_timestamp con formato") {
    intercept[IllegalArgumentException] {
      FieldFunctionRegistry.create(AddFieldDef("x", "noExiste"))
    }
    // No debe lanzar: el formato es opcional y valido
    FieldFunctionRegistry.create(AddFieldDef("dt", "current_timestamp", Map("format" -> "yyyy-MM-dd HH:mm:ss")))
  }

  // ---------- Nombrado de ficheros de salida (unidad) ----------

  test("el nombre base se deriva de la ruta de la fuente") {
    assert(OutputFileNaming.sourceBaseName("data/input/events/person") == "person")
    assert(OutputFileNaming.sourceBaseName("data/input/events/person/person.json") == "person")
    assert(OutputFileNaming.sourceBaseName("/data/input/events/person/*") == "person")
  }

  test("el fichero publicado se nombra <fuente>_ok|ko_<yyyyMMdd_HHmm>.<ext>") {
    val context = OutputNamingContext("person", "20261016_1013")
    val okSink  = SinkDef("raw-ok", "ok_with_date", Seq("out"), "JSON", publication = Some(PublicationDef()))
    val koSink  = SinkDef("raw-ko", "validation_ko", Seq("out"), "JSON", publication = Some(PublicationDef()))
    assert(OutputFileNaming.fileNameFor(okSink, context) == "person_ok_20261016_1013.json")
    assert(OutputFileNaming.fileNameFor(koSink, context) == "person_ko_20261016_1013.json")
    // Override explicito del prefijo en la publicacion
    val custom = okSink.copy(publication = Some(PublicationDef(filePrefix = Some("personas_validas"))))
    assert(OutputFileNaming.fileNameFor(custom, context) == "personas_validas_20261016_1013.json")
  }

  test("la publicacion compone el fichero del consumidor desde varios part-* y lo cataloga") {
    val session = spark
    import session.implicits._
    // Sin coalesce: dos particiones -> dos part-* en el staging de Spark
    val df          = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("id", "amount").repartition(2)
    val base        = Files.createTempDirectory("sdg-framework-publish")
    val destination = base.resolve("out")
    val sink = SinkDef(
      "raw-ok",
      "amounts_ok",
      Seq(destination.toString),
      "JSON",
      publication = Some(PublicationDef(filePrefix = Some("amounts_ok")))
    )
    val context    = OutputNamingContext("amounts", "20261016_1013")
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    val written   = SinkWriter.write(df, sink, "test-publish")
    val published = OutputPublisher.publish(written, context, hadoopConf)
    val catalog =
      OutputCatalog.write(published, hadoopConf, Clock.fixed(Instant.parse("2026-10-16T10:13:00Z"), ZoneId.of("UTC")))

    assert(published.size == 1)
    assert(published.head.records == 3)
    val publishedFile = destination.resolve("amounts_ok_20261016_1013.json")
    assert(Files.isRegularFile(publishedFile))
    assert(spark.read.json(publishedFile.toString).count() == 3)
    assert(catalog.size == 1)
    val catalogFile = destination.resolve("amounts_ok_20261016_1013.catalog.json")
    assert(Files.isRegularFile(catalogFile))
    val catalogContent = new String(Files.readAllBytes(catalogFile), StandardCharsets.UTF_8)
    assert(catalogContent.contains("amounts_ok_20261016_1013.json"))
    assert(catalogContent.contains("\"records\" : 3"))
    val names = Files.list(destination).toArray.map(_.toString)
    assert(
      !names.exists(n => n.contains("part-") || n.contains("_staging")),
      s"Restos en destino: ${names.mkString(", ")}"
    )
  }

  // ---------- Dataflow completo (integracion) ----------

  test("el dataflow del enunciado genera STANDARD_OK y STANDARD_KO correctos") {
    val base     = Files.createTempDirectory("sdg-framework-test")
    val inputDir = Files.createDirectories(base.resolve("input/person"))
    val okPath   = base.resolve("output/person")
    val koPath   = base.resolve("output/discards/person")

    val input =
      """{"name":"Xabier","age":39,"office":""}
        |{"name":"Miguel","office":"RIO"}
        |{"name":"Fran","age":31,"office":"RIO"}""".stripMargin
    Files.write(inputDir.resolve("person.json"), input.getBytes(StandardCharsets.UTF_8))

    val metadataJson =
      s"""{
         |  "dataflows": [{
         |    "name": "prueba-acceso",
         |    "sources": [{
         |      "name": "person_inputs",
         |      "path": "${inputDir.toString.replace("\\", "\\\\")}",
         |      "format": "JSON", "preserveFieldOrder": true
         |    }],
         |    "transformations": [
         |      { "name": "validation", "type": "validate_fields",
         |        "params": { "input": "person_inputs", "validations": [
         |          { "field": "office", "validations": ["notEmpty"] },
         |          { "field": "age", "validations": ["notNull"] }
         |        ] } },
         |      { "name": "ok_with_date", "type": "add_fields",
         |        "params": { "input": "validation_ok", "addFields": [
         |          { "name": "dt", "function": "current_timestamp",
         |            "format": "yyyy-MM-dd HH:mm:ss" }
         |        ] } },
         |      { "name": "ko_with_date", "type": "add_fields",
         |        "params": { "input": "validation_ko", "addFields": [
         |          { "name": "dt", "function": "current_timestamp",
         |            "format": "yyyy-MM-dd HH:mm:ss" }
         |        ] } }
         |    ],
         |    "sinks": [
         |      { "input": "ok_with_date", "name": "raw-ok", "publication": {},
         |        "paths": ["$okPath"], "format": "JSON", "saveMode": "OVERWRITE" },
         |      { "input": "ko_with_date", "name": "raw-ko", "publication": {},
         |        "paths": ["$koPath"], "format": "JSON", "saveMode": "OVERWRITE" }
         |    ]
         |  }]
         |}""".stripMargin
    val metadataPath = base.resolve("metadata.json")
    Files.write(metadataPath, metadataJson.getBytes(StandardCharsets.UTF_8))

    // Reloj fijo: la marca de ejecucion debe ser 20261016_1013 en OK y KO
    val fixedClock = Clock.fixed(Instant.parse("2026-10-16T10:13:00Z"), ZoneId.of("UTC"))
    new DataflowRunner(spark, fixedClock).run(MetadataLoader.load(metadataPath.toString))

    // Los destinos contienen el fichero publicado y su catalogo
    val okFile = okPath.resolve("person_ok_20261016_1013.json")
    val koFile = koPath.resolve("person_ko_20261016_1013.json")
    assert(Files.isRegularFile(okFile), s"No existe el fichero OK esperado: $okFile")
    assert(Files.isRegularFile(koFile), s"No existe el fichero KO esperado: $koFile")
    Seq(okPath, koPath).foreach { dir =>
      val names = Files.list(dir).toArray.map(_.toString)
      assert(
        !names.exists(n => n.contains("part-") || n.contains("_staging")),
        s"Quedan restos de Spark en $dir: ${names.mkString(", ")}"
      )
    }

    // Orden de campos en el fichero: el del input (name, age, office),
    // despues dt; en el KO, ademas, el error en ultima posicion.
    val okLines = Files.readAllLines(okFile)
    assert(okLines.size() == 1)
    assertKeyOrder(okLines.get(0), Seq("name", "age", "office", "dt"))
    val koLines = Files.readAllLines(koFile)
    assert(koLines.size() == 2)
    koLines.forEach { line =>
      assertKeyOrder(line, Seq("name", "age", "office", "dt", "arraycoderrorbyfield"))
    }

    // STANDARD_OK: solo Fran, con dt anadido
    val ok = spark.read.json(okFile.toString)
    assert(ok.count() == 1)
    val okRow = ok.collect()(0)
    assert(okRow.getAs[String]("name") == "Fran")
    assert(okRow.getAs[Long]("age") == 31L)
    assert(okRow.getAs[String]("office") == "RIO")
    assert(okRow.getAs[String]("dt") != null && okRow.getAs[String]("dt").nonEmpty)

    // STANDARD_KO: Xabier (office vacio) y Miguel (age ausente), con causa
    // por campo y con dt
    val ko = spark.read.json(koFile.toString)
    assert(ko.count() == 2)
    val byName = ko.collect().map(r => r.getAs[String]("name") -> r).toMap

    val XabierErrors = errorsOf(byName("Xabier"))
    assert(
      XabierErrors.get("office").exists(_.contains("EMPTY_NOT_ALLOWED")),
      s"Errores de Xabier inesperados: $XabierErrors"
    )

    val miguelErrors = errorsOf(byName("Miguel"))
    assert(
      miguelErrors.get("age").exists(_.contains("NULL_NOT_ALLOWED")),
      s"Errores de Miguel inesperados: $miguelErrors"
    )

    assert(byName("Xabier").getAs[String]("office") == "")
  }

  test("el mismo motor ejecuta otro dataflow distinto solo cambiando metadatos") {
    // Prueba de que no hay logica ad-hoc: otro origen, otras reglas, otra salida.
    val session = spark
    import session.implicits._
    val df        = Seq(("a", 1), ("b", -1)).toDF("id", "amount")
    val base      = Files.createTempDirectory("sdg-framework-generic")
    val inputPath = base.resolve("in")
    val okPath    = base.resolve("ok")
    df.write.json(inputPath.toString)

    val metadataJson =
      s"""{
         |  "dataflows": [{
         |    "name": "amounts",
         |    "sources": [{ "name": "src", "path": "$inputPath", "format": "JSON" }],
         |    "transformations": [
         |      { "name": "check", "type": "validate_fields",
         |        "params": { "input": "src", "validations": [
         |          { "field": "amount", "validations": [{ "rule": "positive" }] }
         |        ] } }
         |    ],
         |    "sinks": [
         |      { "input": "check_ok", "name": "ok", "paths": ["$okPath"],
         |        "format": "JSON", "saveMode": "OVERWRITE" }
         |    ]
         |  }]
         |}""".stripMargin
    val metadataPath = base.resolve("metadata.json")
    Files.write(metadataPath, metadataJson.getBytes(StandardCharsets.UTF_8))

    new DataflowRunner(spark).run(MetadataLoader.load(metadataPath.toString))
    val ok = readJson(okPath)
    assert(ok.count() == 1)
    assert(ok.collect()(0).getAs[String]("id") == "a")
  }

  // ---------- helpers ----------

  /** Comprueba que las claves presentes aparecen en el orden dado. La busqueda de los campos de datos se hace solo en
    * la parte anterior a `arraycoderrorbyfield`, porque dentro del mapa de errores vuelven a aparecer nombres de campo.
    */
  private def assertKeyOrder(line: String, keysInOrder: Seq[String]): Unit = {
    val head = line.split(",\"arraycoderrorbyfield\"")(0)
    val present = keysInOrder.filter { k =>
      if (k == "arraycoderrorbyfield") line.contains("\"" + k + "\"") else head.contains("\"" + k + "\"")
    }
    val positions = present.map { k =>
      if (k == "arraycoderrorbyfield") line.indexOf("\"" + k + "\"") else head.indexOf("\"" + k + "\"")
    }
    assert(positions == positions.sorted, s"Orden de campos incorrecto en: $line")
  }

  private def readJson(path: java.nio.file.Path): DataFrame =
    spark.read.json(path.toString)

  private def errorsOf(row: org.apache.spark.sql.Row): Map[String, Seq[String]] = {
    // En memoria la columna es MapType; al releerla desde JSON, Spark la
    // infiere como struct (claves conocidas). Se aceptan ambas formas.
    def asStrings(v: Any): Seq[String] = v match {
      case null      => Seq.empty
      case s: Seq[_] => s.map(_.toString)
      case other     => Seq(other.toString)
    }
    row.get(row.fieldIndex("arraycoderrorbyfield")) match {
      case null => Map.empty
      case m: scala.collection.Map[_, _] =>
        m.map { case (k, v) => k.toString -> asStrings(v) }.toMap
      case r: org.apache.spark.sql.Row =>
        r.schema.fieldNames.map(f => f -> asStrings(r.getAs[Any](f))).toMap
      case other =>
        throw new AssertionError(s"arraycoderrorbyfield inesperado: $other")
    }
  }
}
