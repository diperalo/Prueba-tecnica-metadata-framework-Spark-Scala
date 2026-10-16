# SDG Group — Prueba técnica: framework de ingesta, validación y transformación dirigido por metadatos

**Posición:** Palantir Engineer · **Stack:** Spark 3.5 / Scala 2.12 / sbt

Implementación de un **framework genérico dirigido por metadatos/configuración**: el programa no contiene ninguna lógica específica de “person”, de los campos `office`/`age` ni de la columna `dt`. Todo eso vive en `config/metadata.json`. Cambiar el origen, las validaciones, los campos derivados o los destinos **no requiere tocar ni recompilar código**: basta con cambiar los metadatos y volver a ejecutar el mismo binario.

---

## 1. Qué hace (requisitos del enunciado)

| Requisito | Cómo se cumple |
|---|---|
| Cargar los orígenes descritos en los metadatos | `SourceReader`: lectura genérica por `format` + `options` (JSON, CSV, Parquet…) |
| Ejecutar las transformaciones del *dataflow* | `DataflowRunner` + `TransformationRegistry`: ejecuta en orden los pasos declarados, resolviendo cada `input` por nombre lógico |
| Dos validaciones sobre dos campos (`office`: `notEmpty`, `age`: `notNull`) | Transformación genérica `validate_fields` + `ValidationRegistry` |
| Añadir una columna (`dt` = `current_timestamp`) | Transformación genérica `add_fields` + registro de funciones |
| Escribir los que pasan la validación (`STANDARD_OK`) | Sink `raw-ok` ← dataset `ok_with_date` |
| Escribir los que no la pasan (`STANDARD_KO`) con el código de error que autoexplique el motivo | Sink `raw-ko` ← dataset `ko_with_date` (la rama KO también añade `dt`, como en el fichero ejemplarizante), con columna `arraycoderrorbyfield` |
| Programa dirigido por metadatos, **no ad-hoc** | Ver §3: el motor desconoce el dominio; hay un test que ejecuta un dataflow completamente distinto (otro origen, otra regla `positive`, otra salida) con el mismo binario |

### Datos de ejemplo

**Entrada** (`data/input/events/person/person.json`):

```json
{"name":"Xabier","age":39,"office":""}
{"name":"Miguel","office":"RIO"}
{"name":"Fran","age":31,"office":"RIO"}
```

**Salida `STANDARD_OK`** — fichero único `data/output/events/person/person_ok_<ejecución>.json`, solo Fran, con `dt`. Contenido real de la ejecución de verificación (el orden de claves es el del esquema inferido por Spark; en JSON es irrelevante):

```json
{"name":"Fran","age":31,"office":"RIO","dt":"2026-10-16 10:06:36"}
```

> La fecha/hora es la de ejecución (`current_timestamp`); el formato `yyyy-MM-dd HH:mm:ss` se declara en los metadatos (`"format"`), igual que en el ejemplo del enunciado (`2020-12-29 09:00:00`). Sin `format`, la columna quedaría como `timestamp` nativo de Spark.

**Salida `STANDARD_KO`** — fichero único `data/output/discards/person/person_ko_<ejecución>.json`. Cada registro conserva sus datos originales y añade el mapa *campo → códigos de error* (solo aparecen los campos que fallan). Contenido real de la misma ejecución:

```json
{"name":"Xabier","age":39,"office":"","dt":"2026-10-16 10:06:37","arraycoderrorbyfield":{"office":["EMPTY_NOT_ALLOWED"]}}
{"name":"Miguel","age":null,"office":"RIO","dt":"2026-10-16 10:06:37","arraycoderrorbyfield":{"age":["NULL_NOT_ALLOWED"]}}
```

> Notas: (1) `Miguel` no traía `age` en la entrada; la validación materializa el campo ausente como `null` antes de validar, por eso `notNull` puede detectarlo. (2) El sink KO declara `"options": {"ignoreNullFields": "false"}` para que ese `null` se escriba explícitamente (por defecto el *writer* JSON de Spark omite los campos nulos).

### Orden de los campos en los ficheros de salida

El orden de los campos **es el del fichero de entrada**, no el alfabético que infiere Spark al leer JSON sin esquema (`age, name, office`). Regla completa:

1. Campos del registro en su orden de entrada —la fuente lo declara con `"preserveFieldOrder": true` (en JSON se muestrea el fichero y se recogen las claves en orden de primera aparición; un campo que solo apareciera en registros posteriores iría tras los muestreados, nunca se pierde). En CSV el orden ya es el de la cabecera.
2. Despues, los campos anadidos por las transformaciones en el orden en que los metadatos los declaran (p. ej. `dt`, y en el ejemplo 03 `sku_upper`).
3. En el KO, `arraycoderrorbyfield` siempre en **ultima posicion**, despues de `dt`: es un apendice del registro y la escritura lo recoloca al final aunque se anadan columnas despues de validar.

Ejemplo: `{"name":"Fran","age":31,"office":"RIO","dt":"..."}` y en el KO `{"name":"Xabier","age":39,"office":"","dt":"...","arraycoderrorbyfield":{...}}`, tal como muestran los ficheros ejemplarizantes del enunciado. La comprobacion de orden es automatica: los tests lo verifican sobre el texto del fichero y `ExamplesSpec` compara tambien la lista ordenada de claves de cada registro en los dataflows ejemplarizantes.

### Nombre de los ficheros de salida: publicación y catalogación

El consumidor de estas salidas no consume directorios `part-*` de Spark: necesita **un fichero con un nombre concreto**. Esa es una necesidad de entrega, no de procesamiento, así que el proceso tiene fases separadas:

1. **Escritura (Spark nativo).** Las transformaciones y el writer trabajan como siempre: cada dataset se escribe como directorio con sus `part-*`, en paralelo, sin forzar ningún `coalesce`. Si el sink se va a publicar, esa escritura va a un área interna `_staging/` del destino.
2. **Publicación (`OutputPublisher`).** Después de escribir, esta fase compone el fichero del consumidor concatenando los `part-*` (formatos orientados a líneas: JSON, CSV, TEXT) con el nombre `<prefijo>_<yyyyMMdd_HHmm>.<extensión>`, contando registros y bytes, y retira el staging. Antes los `part-*` llegaban al consumidor y el nombre concreto no existía; ahora el nombre es parte del contrato de entrega sin tocar el cálculo.
3. **Catalogación (`OutputCatalog`).** Junto a cada fichero publicado queda su entrada de catálogo, `person_ok_20261016_1013.catalog.json`, con dataflow, sink, dataset de origen, estado, nombre, ruta, formato, marca de ejecución, instante de publicación, nº de registros, bytes y esquema: un consumidor puede descubrir y validar lo publicado sin abrir el fichero de datos.

```
data/output/events/person/person_ok_20261016_1013.json          <- datos (STANDARD_OK)
data/output/events/person/person_ok_20261016_1013.catalog.json  <- catálogo
data/output/discards/person/person_ko_20261016_1013.json        <- datos (STANDARD_KO)
data/output/discards/person/person_ko_20261016_1013.catalog.json
```

- El **prefijo se deriva de los metadatos**, no se codifica: nombre base de la fuente (`person.json`, el directorio `.../person` o el patrón `.../person/*` → `person`) + sufijo `_ok`/`_ko` deducido del sink (`raw-ok`/`ok_with_date`, `raw-ko`/`ko_with_date`). La publicación puede fijar otro prefijo con `"filePrefix"`.
- La **marca de ejecución** (`yyyyMMdd_HHmm`) se calcula una sola vez por dataflow desde un `java.time.Clock` inyectado en el runner: OK y KO comparten sufijo, y el nombrado es testeable con un reloj fijo (los tests generan literalmente `person_ok_20261016_1013.json`).
- Se declara por sink con `"publication": {}` (opciones: `filePrefix`, `catalog: false` para no catalogar). Sin `publication`, el sink entrega el directorio nativo de Spark tal cual: la publicación es optativa por consumidor, el procesamiento es el mismo.
- El antiguo `"singleFileOutput": true` se sigue aceptando como alias de `"publication": {}`.

---

## 2. Entorno y cómo ejecutarlo (Windows + IntelliJ IDEA)

**Entorno de trabajo:** Windows 10/11 · IntelliJ IDEA (Community o Ultimate) con el plugin de Scala · JDK 17 (también funciona con 11) · sbt (el que trae IntelliJ o el instalado en el sistema) · PowerShell (terminal integrada de IntelliJ). Todos los scripts del repositorio son `.ps1` para ese entorno.

### Apertura en IntelliJ IDEA

> **Importante: abre la carpeta que contiene directamente `build.sbt`, `src` y `run.ps1`.**
> Al descomprimir el ZIP, Windows puede crear una carpeta contenedora (p. ej.
> `SDG-prueba-tecnica-Spark-Scala\sdg-metadata-framework`). En ese caso, la carpeta que debes abrir
> en IntelliJ es la **interior**, `sdg-metadata-framework`, no la contenedora. Si en la vista
> *Project* ves otra carpeta `sdg-metadata-framework` dentro de la raíz, has abierto la carpeta
> equivocada: ciérralo y abre la interior. La raíz correcta muestra `build.sbt` y `run.ps1`
> directamente.

1. *File → Open…* sobre la carpeta interior del proyecto y *Trust Project*. IntelliJ debería detectar
   `build.sbt` y proponer importarlo como proyecto sbt; acepta la importación y espera a que termine
   la indexación.
2. Si no aparece la propuesta de importación, no dependas del clic derecho sobre `build.sbt` (en
   algunas versiones esa opción no se muestra): abre *View → Tool Windows → sbt*, pulsa **+**
   (*Attach sbt Project*), selecciona el `build.sbt` de la raíz del proyecto y confirma. La ventana
   sbt debe listar el proyecto `sdg-metadata-framework`.
3. *File → Project Structure → Project*: SDK = JDK 17 (p. ej. Temurin o Microsoft OpenJDK) y lenguaje 17.
   En *Project Structure → Modules* debe existir el módulo `sdg-metadata-framework` creado por la
   importación sbt; si no existe ningún módulo, la importación no se ha hecho y las ejecuciones
   darán `ClassNotFoundException: com.sdg.framework.Main`.
4. En *Settings → Build, Execution, Deployment → Build Tools → sbt*: JRE = *Default (project SDK)*.
   Desde la ventana *sbt* (o el terminal integrado, que en Windows es PowerShell) se ejecutan las
   tareas con los alias del proyecto: `t` (test), `c` (compile), `f` (formatear), `prep`.
5. Formato: en *Settings → Editor → Code Style*, despliega **Code Style** y entra en **Scala** (no en
   *General*): *Formatter: Scalafmt* para que el IDE use el mismo `.scalafmt.conf` que sbt (opción
   *Reformat on Save* opcional). Si no existe la entrada *Scala*, activa el plugin de Scala en
   *Settings → Plugins* y reinicia IntelliJ.
6. Antes de usar las configuraciones de ejecución, compila una vez (*Build → Build Project* o la
   tarea `compile` en la ventana sbt). Debe generarse
   `target\scala-2.12\classes\com\sdg\framework\Main.class`.
7. La carpeta `.idea\` es local de cada desarrollador y no se versionea; lo compartido son las
   configuraciones de ejecución de `.run\` (ver siguiente punto).

#### Si aparece `ClassNotFoundException: com.sdg.framework.Main`

Significa que la JVM arranca pero el módulo no se ha compilado porque el proyecto sbt no está
importado (suele pasar al abrir la carpeta contenedora en lugar de la raíz del proyecto). Comprueba,
en este orden: (1) la raíz abierta contiene `build.sbt` directamente; (2) existe el módulo
`sdg-metadata-framework` en *Project Structure → Modules*; (3) la ventana *sbt* lista el proyecto;
(4) tras compilar existe `target\scala-2.12\classes\com\sdg\framework\Main.class`. La
configuración de ejecución debe usar ese módulo y directorio de trabajo = raíz del proyecto
(`$PROJECT_DIR$`).

#### Requisitos de Windows para ejecutar Spark en local: sbt y Hadoop nativo

Dos mensajes típicos al estrenar el proyecto en Windows, con causas distintas:

- **`El término 'sbt' no se reconoce` al lanzar `.\run.ps1`.** El sbt que usa IntelliJ para
  importar el proyecto no instala ningún comando `sbt` en el PATH de PowerShell. Las
  configuraciones de ejecución de IntelliJ y la ventana *sbt* funcionan sin él, pero los scripts
  `.ps1` invocan el comando `sbt`. Al ejecutarlos, primero buscan sbt en el PATH y en las rutas
  habituales de instalación; si no lo encuentran, **preguntan si quieres instalarlo**
  (`[S/n]`: Enter o `S` = sí, `N` = no). Si aceptas, ejecutan
  `winget install --id sbt.sbt -e --accept-package-agreements --accept-source-agreements`,
  refrescan el PATH de la sesión, localizan `sbt.bat` y **continúan con la ejecución** sin tener
  que volver a lanzar el script. Si rechazas, paran con las instrucciones manuales
  (`winget install --id sbt.sbt -e` y reabrir la terminal; también sirve Scoop/Chocolatey o el
  instalador oficial de sbt).

- **`Did not find winutils.exe` / `UnsatisfiedLinkError: NativeIO$Windows.access0`.** Spark
  3.5.1 trae el cliente Hadoop 3.3.4, y Hadoop en Windows necesita sus binarios nativos:
  `winutils.exe` y `hadoop.dll`. Ojo: el repositorio comunitario `cdarlint/winutils` no publica
  binarios 3.3.4 (salta de 3.2.2 a 3.3.5/3.3.6), así que los scripts descargan los del *patch*
  **3.3.6** (con 3.3.5 como reserva) de la misma línea 3.3.x: el punto de entrada nativo que usa
  Spark, `NativeIO$Windows.access0`, no cambia entre *patch releases* y Hadoop no exige igualdad
  exacta de *patch* al cargar la librería. Los scripts los comprueban antes de
  lanzar Spark: si faltan en `C:\hadoop\bin` (o en la carpeta de `HADOOP_HOME`, si ya está
  definida), **preguntan si quieres descargarlos y configurar el entorno** (`[S/n]`). Si aceptas,
  crean la carpeta, descargan ambos binarios del repositorio comunitario `cdarlint/winutils`
  (`hadoop-3.3.6/bin`), definen `HADOOP_HOME` como variable de usuario, añaden
  `%HADOOP_HOME%\bin` al PATH de usuario, lo aplican a la sesión actual, comprueban que ambos
  ficheros existen, ejecutan `winutils.exe` como prueba y **continúan con la ejecución**. Si
  rechazas, paran antes de lanzar Spark. La configuración manual equivalente es:

  ```powershell
  New-Item -ItemType Directory -Force "C:\hadoop\bin" | Out-Null
  Invoke-WebRequest "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe" -OutFile "C:\hadoop\bin\winutils.exe"
  Invoke-WebRequest "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll" -OutFile "C:\hadoop\bin\hadoop.dll"
  [Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
  $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
  if ($userPath -notlike "*%HADOOP_HOME%\bin*") {
      [Environment]::SetEnvironmentVariable("Path", "$userPath;%HADOOP_HOME%\bin", "User")
  }
  $env:HADOOP_HOME = "C:\hadoop"
  $env:Path = "$env:HADOOP_HOME\bin;$env:Path"
  Test-Path "$env:HADOOP_HOME\bin\winutils.exe"   # debe devolver True
  Test-Path "$env:HADOOP_HOME\bin\hadoop.dll"     # debe devolver True
  & "$env:HADOOP_HOME\bin\winutils.exe"           # imprime su ayuda, sin error
  ```

  Ojo: la pregunta automática de Hadoop solo ocurre al ejecutar los scripts `.ps1`; el botón ▶
  de IntelliJ no pasa por ellos. Para usar ▶, las configuraciones de `.run\` ya fijan la
  variable de entorno `HADOOP_HOME=C:\hadoop` y las VM options `-Dhadoop.home.dir=C:\hadoop`
  y `-Djava.library.path=C:\hadoop\bin` (esta última es la que permite a
  `System.loadLibrary("hadoop")` encontrar `hadoop.dll`), así que no dependen del PATH de la
  sesión de IntelliJ. Aun así, si el aviso persiste con un IntelliJ abierto desde antes de
  instalar los binarios, ciérralo y vuelve a abrirlo. Si instalas los binarios en otra carpeta,
  ajusta esas rutas en la configuración de ejecución. Sin los binarios, Spark arranca pero
  falla en cuanto Hadoop lista el sistema de ficheros local, como se ve en el stack trace `RawLocalFileSystem.listStatus`.

#### Si PowerShell dice que `run.ps1` no se reconoce

Estás en la carpeta contenedora, no en la raíz del proyecto. En la terminal:

```powershell
cd .\sdg-metadata-framework
.\run.ps1
```

(ajusta la ruta si tu carpeta raíz tiene otro nombre; `Test-Path .\build.sbt` debe devolver `True`
antes de ejecutar el script).

### Ejecución desde IntelliJ

En `.run\` van compartidas cuatro configuraciones de tipo *Application* (clase `com.sdg.framework.Main`, directorio de trabajo = raíz del proyecto, ya con las opciones JVM `--add-opens` que Spark necesita en Java 17): aparecen en el desplegable de ejecuciones tras importar el proyecto.

| Configuración | Argumento (metadatos) |
|---|---|
| `Prueba tecnica` | `config\metadata.json` (con `/`: `config/metadata.json`) |
| `Ejemplo 01 - Validaciones multiples` | `examples/01-validaciones-multiples/metadata.json` |
| `Ejemplo 02 - Ordenes CSV` | `examples/02-ordenes-csv/metadata.json` |
| `Ejemplo 03 - Transformaciones genericas` | `examples/03-transformaciones-genericas/metadata.json` |

Con ▶ se ejecuta el proceso completo (y con el depurador, poniendo breakpoints en las transformaciones); el resultado queda en `data\output\...` y se puede abrir desde la propia vista del proyecto. Para los tests: clic derecho sobre `FrameworkSpec` → *Run 'FrameworkSpec'*, o `sbt t` en la ventana sbt.

> Las rutas dentro de los metadatos se escriben con `/` a propósito: la JVM y Spark las aceptan igual en Windows, y así los mismos metadatos sirven sin cambios si el proceso acaba lanzándose en un cluster.

### Ejecución desde la terminal integrada (PowerShell)

La terminal de IntelliJ (*View → Tool Windows → Terminal*) abre PowerShell en la raíz del proyecto:

```powershell
# Si la politica de ejecucion bloquea los scripts locales (solo hace falta una vez por sesion):
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
# Alternativa sin tocar la politica: .
un.cmd lanza run.ps1 con -ExecutionPolicy Bypass

.\run.ps1                                    # ejecuta el ejercicio y muestra STANDARD_OK / STANDARD_KO
.\run.ps1 config/metadata.json               # con otros metadatos
.\examples\run_examples.ps1                  # los 4 dataflows + verificacion de las 8 salidas
.\doc\hooks\install-hooks.ps1                # instala el hook pre-push (instala Git si falta; si aun no es repo, ofrece clonar el remoto o hacer git init con primer commit)
.\git-commit.ps1                             # asistente guiado de commit y push (propone el mensaje; integra lo remoto antes de subir)

sbt test                                     # alias: sbt t
sbt f                                        # formatear con scalafmt; sbt fc solo comprueba
sbt prep                                     # compilar + compilar tests + check de formato
sbt "run config/metadata.json"               # ejecucion directa sin script
sbt "run config/metadata.json --dataflow=prueba-acceso"   # solo un dataflow
sbt test                                     # incluye ExamplesSpec: verifica los ejemplos contra los expected
```

> La verificación de los ejemplos es parte de la batería de tests: `ExamplesSpec` ejecuta los tres
> dataflows ejemplarizantes sobre directorios temporales y comprueba fichero publicado, catálogo,
> contenido y orden de campos contra los `expected`. Basta `sbt test` (o el paso final de
> `run_examples.ps1`), sin dependencias fuera del stack Scala/sbt.

Los `.ps1` paran ante el primer error y propagan el código de salida de sbt; están escritos solo con caracteres ASCII para que Windows PowerShell 5.1 los lea sin problemas de codificación.

Despliegue tipo cluster (fuera del alcance Lite): pasar antes `spark-core`/`spark-sql` a `"provided"` en `project/Dependencies.scala` (el cluster aporta Spark), empaquetar con `sbt assembly` y enviar el jar con `spark-submit`.

Alias definidos en `project/SbtAliases.scala`: `t` test, `to` testOnly, `c` compile, `tc` Test/compile, `f` scalafmt, `fc` scalafmtCheck, `tf`/`tfc` lo mismo para tests, `prep` = `;c;tc;fc;tfc`. El hook `doc/hooks/pre-push` ejecuta `sbt prep` antes de cada push (Git para Windows lo ejecuta con el shell que trae el propio Git; se instala con `.\doc\hooks\install-hooks.ps1`). El asistente `git-commit.ps1` (en la raíz) muestra los cambios pendientes, propone un mensaje de commit que puedes aceptar o editar y, si decides subirlo, hace antes `git fetch` e integra los cambios remotos con *rebase*; nunca fuerza el push: si la integración produjera un conflicto, se detiene, nombra los ficheros afectados y no sube nada.

> Verificación realizada: `sbt prep` y la batería de tests (**12/12**, incluidos los tres ejemplos verificados por `ExamplesSpec`) en verde; la ejecución genera los ficheros publicados y catalogados `person_ok_<yyyyMMdd_HHmm>.json` / `person_ko_<yyyyMMdd_HHmm>.json` con los contenidos de §1 (en `data/output/`, sin `part-*` a la vista del consumidor). Además hay **ficheros ejemplarizantes adicionales** en `examples/` (validaciones múltiples parametrizadas, fuente CSV y transformaciones genéricas encadenadas), cada uno con su entrada, su salida esperada y comprobación automática: `.\examples\run_examples.ps1` ejecuta los 4 dataflows y verifica las 8 salidas (ver `examples/README.md`).

Las rutas de `config/metadata.json` son relativas al directorio del proyecto para poder ejecutarlo en local. En un entorno real serían rutas absolutas/HDFS/S3 (`/data/input/events/person/*`, etc., como en la plantilla del enunciado): es solo un cambio en los metadatos.

---

## 3. Diseño: por qué no es un desarrollo ad-hoc

```
config/metadata.json
        │
        ▼
 MetadataLoader ──► Metadata{ dataflows[] }
        │                 (los params de cada transformación se conservan
        │                  como JSON sin tipar: el núcleo no los conoce)
        ▼
 DataflowRunner ──► catálogo: Map[nombre lógico, DataFrame]
        │
        ├─ sources         ──► SourceReader   (formato/opciones de metadatos)
        ├─ transformations ──► TransformationRegistry[tipo]   (en orden)
        │        ├─ validate_fields ──► <name>_ok / <name>_ko
        │        │        └─ ValidationRegistry[regla] → códigos de error
        │        ├─ add_fields      ──► <name>
        │        │        └─ FieldFunctionRegistry[función]
        │        ├─ rename_fields / drop_fields / filter ──► <name>
        │        └─ … (nuevas: implementar Transformation + registrar)
        ├─ sinks           ──► SinkWriter     (Spark nativo: directorios part-*)
        └─ publicación     ──► OutputPublisher (fichero del consumidor, nombre concreto)
                               OutputCatalog   (<fichero>.catalog.json)
```

Decisiones clave:

1. **Catálogo por nombre lógico.** Cada fuente, salida de transformación y sink se referencia por nombre (`person_inputs` → `validation_ok` → `ok_with_date`, `validation_ko`). El encadenado es exactamente el de la plantilla del enunciado, pero resuelto genéricamente.
2. **`params` sin tipar en el modelo.** `TransformationDef` guarda los params como árbol JSON; cada transformación valida e interpreta los suyos. Añadir un tipo nuevo no obliga a modificar el modelo ni el *runner* (principio abierto/cerrado).
3. **Registros extensibles** para los tres ejes de variabilidad: tipos de transformación, reglas de validación y funciones de campos derivados. Un tipo/regla/función desconocido produce un error explícito que lista los disponibles.
4. **Convención de salidas de `validate_fields`:** `<nombre>_ok` y `<nombre>_ko`. Con `"name": "validation"` se obtienen `validation_ok` / `validation_ko`, que es lo que consumen el siguiente paso y el sink de descartes de la plantilla.
5. **Errores autoexplicativos y estructurados**, no texto libre: `arraycoderrorbyfield` es un `map<string, array<string>>` consultable aguas abajo (p. ej. para métricas de calidad por campo y regla sin re-parsear logs).
6. **`saveMode`, `format`, `options`, `partitionBy` y rutas múltiples** (`paths`) salen de los metadatos y se aplican con el `DataFrameWriter` genérico.
7. **Entrega desacoplada del procesamiento, y testeable.** La escritura es siempre nativa de Spark. Encima, `OutputFileNaming` concentra las reglas de nombre del fichero del consumidor (prefijo derivado de la fuente, sufijo `_ok`/`_ko`, extensión según formato), `OutputPublisher` lo publica tras la escritura y `OutputCatalog` lo cataloga. El `DataflowRunner` recibe un `java.time.Clock` inyectado: la marca `yyyyMMdd_HHmm` sale del reloj, en producción el del sistema y en los tests uno fijo (`20261016_1013`).

### Metadatos del ejercicio (`config/metadata.json`)

Es la plantilla del enunciado con tres únicos ajustes: rutas relativas (ejecutable sin `/data`), `"format"` en `dt` para reproducir el formato de fecha del ejemplo, y una rama `ko_with_date` simétrica a `ok_with_date`. La plantilla conectaba el sink KO directamente a `validation_ko`, pero los ficheros ejemplarizantes de salida muestran `dt` **también en el KO**, así que ambas ramas añaden la columna antes de escribirse (la salida manda sobre la plantilla en ese detalle). La forma de declarar validaciones admite también parámetros sin romper la forma simple:

```json
"validations": [
  { "field": "office", "validations": ["notEmpty"] },
  { "field": "age",    "validations": ["notNull", { "rule": "minValue", "value": "0" }] }
]
```

### Transformaciones disponibles

| `type` | Salidas | Params principales |
|---|---|---|
| `validate_fields` | `<name>_ok`, `<name>_ko` | `input`, `validations[]` (`field` + reglas) |
| `add_fields` | `<name>` | `input`, `addFields[]` (`name`, `function`, parámetros de la función) |
| `rename_fields` | `<name>` | `input`, `renames[]` (`from`, `to`) |
| `drop_fields` | `<name>` | `input`, `fields[]` |
| `filter` | `<name>` | `input`, `condition` (expresión Spark SQL) |

### Validaciones disponibles (`ValidationRegistry`)

| Regla (metadatos) | Código en `STANDARD_KO` | Significado |
|---|---|---|
| `notNull` | `NULL_NOT_ALLOWED` | El campo es nulo o no viene en el registro *(validación técnica: integridad del dato)* |
| `notEmpty` / `notBlank` | `EMPTY_NOT_ALLOWED` | El campo es nulo o cadena vacía/solo espacios *(validación funcional: regla de negocio)* |
| `positive` | `NOT_POSITIVE` | Numérico ≤ 0 o nulo |
| `nonNegative` | `NEGATIVE_NOT_ALLOWED` | Numérico < 0 o nulo |
| `minValue` (`value`) | `BELOW_MIN_VALUE` | Numérico por debajo del mínimo |
| `maxValue` (`value`) | `ABOVE_MAX_VALUE` | Numérico por encima del máximo |
| `inSet` / `allowedValues` (`values`, separado por comas) | `VALUE_NOT_IN_ALLOWED_SET` | Fuera del dominio permitido |
| `matchesRegex` / `regex` (`pattern`) | `VALUE_DOES_NOT_MATCH_PATTERN` | No cumple el patrón |
| `minLength` / `maxLength` (`value`) | `VALUE_TOO_SHORT` / `VALUE_TOO_LONG` | Longitud fuera de rango |

Los nombres de regla se normalizan (insensibles a mayúsculas, `_` y `-`): `notNull`, `not_null` y `NOTNULL` equivalen.

### Funciones disponibles para `add_fields` (`FieldFunctionRegistry`)

`current_timestamp`, `current_date` (ambas con `format` opcional), `lit`/`literal` (`value`), `upper`, `lower`, `trim` (con `inputField`).

### Cómo se extiende (sin tocar el núcleo)

- **Nueva validación:** implementar `ValidationRule` y registrar su factory en `ValidationRegistry`.
- **Nueva función:** registrar una entrada en `FieldFunctionRegistry`.
- **Nueva transformación:** implementar el trait `Transformation` y registrarla en `TransformationRegistry` (o con `TransformationRegistry.register(...)` al arrancar).
- **Nuevo formato de E/S:** ninguno: se declara en los metadatos y lo resuelve Spark.
- **Nuevo dataflow:** añadir un objeto a `dataflows` (pueden coexistir varios y seleccionarse con `--dataflow`).

---

## 4. Buenas prácticas aplicadas

Referencia: [CodelyTV/scala-spark-skeleton.g8](https://github.com/CodelyTV/scala-spark-skeleton.g8). Se adoptan sus convenciones de proyecto, adaptadas a este ejercicio:

| Práctica | Aplicación aquí |
|---|---|
| Scalafmt configurado | `.scalafmt.conf` (versión 3.8.4, dialecto `scala212`, 120 columnas, `SortImports`…) + plugin `sbt-scalafmt` en `project/sbt-scalafmt.sbt`. Todo el código está formateado con `sbt f` y `sbt prep` lo verifica |
| EditorConfig | `.editorconfig` (UTF-8, LF, indentación Scala de 2 espacios) |
| `build.sbt` mínimo | Solo compone `Settings.settings`, `Dependencies.all` y los alias. La configuración vive en `project/Settings.scala`, las dependencias y versiones en `project/Dependencies.scala` y los alias en `project/SbtAliases.scala` |
| Plugins sbt por fichero | `project/sbt-scalafmt.sbt` y `project/sbt-assembly.sbt` (antes un único `plugins.sbt`) |
| Alias de tareas | `t`, `to`, `tq`, `c`, `tc`, `f`, `fc`, `tf`, `tfc` y `prep` (checks pre-push) |
| Hook pre-push | `doc/hooks/pre-push` ejecuta `sbt prep`; se instala con `doc\hooks\install-hooks.ps1` |
| Sin subdirectorio `scala/` | Las fuentes viven directamente en `src/main/<paquete>` y `src/test/<paquete>` (`Compile/Test / scalaSource` ajustados en `Settings.scala`): no se parte el código por lenguaje, solo por paquetes |
| Opciones estrictas del compilador | `-deprecation -feature -unchecked -Xlint -Ywarn-dead-code -Ywarn-unused` (+`-Xcheckinit` en tests); la compilación queda sin warnings |
| Tests Spark aislados y deterministas | Sesión local en `beforeAll`, directorios temporales por test y, ahora, `Clock` fijo inyectado para las marcas temporales |
| `.gitignore` solo del proyecto | Sin reglas de IDE/SO (eso va en el `.gitignore` global del desarrollador); solo artefactos del proyecto (`target/`, salidas, warehouse local…) |

Desviaciones conscientes respecto a esa referencia, y por qué:

- **Spark en ámbito `Compile`, no `Provided`**: la versión Lite debe ejecutarse con `sbt run` sin tener Spark instalado. Para un despliegue en cluster se cambia a `Provided` en `project/Dependencies.scala`.
- **Scala 2.12**, alineado con los artefactos de Spark 3.5.1 usados y con el dialecto `scala212` de `.scalafmt.conf`.

## 5. Estructura del proyecto

```
.
├── build.sbt                          # Settings + Dependencies + alias (mínimo)
├── .scalafmt.conf                     # Formato: scalafmt 3.8.4 / scala212 / 120 cols
├── .editorconfig
├── config/
│   └── metadata.json                  # Metadatos del ejercicio
├── data/input/events/person/
│   └── person.json                    # Entrada del enunciado (3 registros)
├── doc/hooks/                         # pre-push (sbt prep) + instalador
├── project/
│   ├── Dependencies.scala             # Dependencias y versiones
│   ├── Settings.scala                 # Nombre, scalaVersion, scalacOptions, forks/JVM…
│   ├── SbtAliases.scala               # t, c, f, fc, prep…
│   ├── sbt-scalafmt.sbt
│   └── sbt-assembly.sbt
├── .run/                              # Configuraciones de ejecución compartidas de IntelliJ
├── examples/                          # 3 dataflows ejemplarizantes extra + verificación (.\examples\run_examples.ps1)
├── data/expected/                     # STANDARD_OK/KO esperados del enunciado
├── run.ps1                            # Atajo: ejecuta y muestra OK/KO
├── run.cmd                            # Lanzador de run.ps1 con -ExecutionPolicy Bypass
└── src/
    ├── main/com/sdg/framework/        # (sin carpeta scala/, ver §4)
    │   ├── Main.scala                 # CLI: args → SparkSession → runner
    │   ├── config/MetadataLoader.scala# Parseo/validación de la carcasa de metadatos
    │   ├── model/Metadata.scala       # Modelo (params como JSON sin tipar)
    │   ├── engine/DataflowRunner.scala# Motor: catálogo + orden + Clock de ejecución
    │   ├── io/
    │   │   ├── Io.scala               # SourceReader / SinkWriter (Spark nativo)
    │   │   ├── OutputFileNaming.scala # person_ok_20261016_1013.json y compañía
    │   │   └── OutputPublisher.scala  # Publicación del fichero + OutputCatalog
    │   ├── transformations/
    │   │   ├── Transformation.scala   # Trait + registro de tipos
    │   │   ├── ValidateFieldsTransformation.scala  # Split OK/KO + errores por campo
    │   │   ├── AddFieldsTransformation.scala       # Campos derivados + registro de funciones
    │   │   └── GenericTransformations.scala        # rename / drop / filter
    │   └── validation/Validation.scala# Reglas + registro (códigos de error)
    └── test/com/sdg/framework/
        ├── FrameworkSpec.scala        # Unidad + integración (nombrado, OK/KO, genericidad)
        └── ExamplesSpec.scala         # Verificación de los 3 ejemplos contra sus expected
```

## 6. Tests

`sbt test` (12 tests) cubre:

- Creación de reglas simples y parametrizadas, y error descriptivo ante reglas/funciones desconocidas.
- **Nombrado de ficheros (unidad):** derivación del nombre base desde ruta a fichero, a directorio o con patrón, y composición `<fuente>_ok|ko_<yyyyMMdd_HHmm>.<ext>` (incluido el override `filePrefix` de la publicación).
- **Publicación y catalogación:** un DataFrame con 2 particiones se escribe nativo (2 `part-*` en staging) y la fase de publicación compone el fichero único del consumidor, lo cataloga con su contador de registros y no deja restos visibles.
- **Integración completa del enunciado:** con un `Clock` fijo, ejecuta el dataflow sobre un directorio temporal con los 3 registros y verifica que existen `person_ok_20261016_1013.json`, `person_ko_20261016_1013.json` y sus `.catalog.json` (sin `part-*` ni `_staging` visibles), que el OK contiene solo a Fran (con `dt`) y que el KO contiene a Xabier con `EMPTY_NOT_ALLOWED` en `office` y a Miguel con `NULL_NOT_ALLOWED` en `age`, **ambos KO también con `dt`** como en el fichero ejemplarizante.
- **Genericidad:** un segundo dataflow, distinto en origen, regla (`positive`) y salidas, ejecutado por el mismo motor solo cambiando los metadatos (y sin `publication`, con la salida nativa de Spark).
- **Ejemplos ejemplarizantes (`ExamplesSpec`):** los tres dataflows de `examples/` se ejecutan sobre directorios temporales con un reloj fijo y se verifica en cada salida el fichero publicado con su nombre, el catalogo con el contador de registros, la ausencia de `part-*`/`_staging`, el contenido frente a los `expected` (con `dt` normalizado) y el orden de campos.

## 7. Limitaciones conscientes y evolución en un entorno real

- La composición del fichero del consumidor en la publicación es secuencial por naturaleza (un solo fichero): su coste es el de copiar la salida ya calculada, no el de procesarla, y solo lo pagan los sinks cuyo consumidor exige un único fichero. Si mañana un consumidor acepta el directorio nativo o un formato columnar particionado, se retira su `publication` y el dataflow (cálculo, validaciones, escritura) no cambia en absoluto; la entrada de catálogo ya deja registrado qué se publicó en cada ejecución para esa transición.
- Dependencias Spark en ámbito *compile* para que `sbt run` funcione sin cluster; en producción se pasarían a `provided` y se desplegaría con `spark-submit`/orquestador.
- Las validaciones anidadas (campos dentro de structs/arrays) y la validación de esquema de entrada (tipos obligatorios) serían el siguiente paso natural, junto con grandes volúmenes: particionado por fecha en los sinks y formato columnar (Parquet/Delta) en lugar de JSON.
- El patrón registro + catálogo permite añadir observabilidad (conteos OK/KO por regla publicados como métricas), cuarentena con reintentos de KO corregidos y linaje por nombre lógico sin cambiar la arquitectura.
