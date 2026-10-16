# Ficheros ejemplarizantes adicionales

Ejemplos ejecutables para comprobar, además del caso del enunciado, que el framework es genérico: cada uno es **solo metadatos + datos de entrada**, sin código nuevo. En todos, OK y KO llevan `dt` y se publican con nombre concreto y entrada de catálogo (`<fichero>.catalog.json`), igual que en el dataflow principal.

## Cómo comprobarlos todos

```powershell
.\examples\run_examples.ps1
# Equivale a lanzar en IntelliJ las 4 configuraciones de .run\ (o sbt "run <metadata>"
# de cada ejemplo desde la terminal integrada) y despues verificar con la bateria de tests:
# sbt "testOnly com.sdg.framework.ExamplesSpec"
```

La verificación es un test ScalaTest (`ExamplesSpec`, en `src/test/`), no un script externo: ejecuta cada dataflow ejemplarizante sobre un directorio temporal con un reloj fijo y comprueba en cada salida el nombre `<prefijo>_ok|ko_<yyyyMMdd_HHmm>.json`, la ausencia de `part-*`/`_staging`, la entrada de catálogo con el contador de registros correcto, el contenido igual a `expected/expected_*.json` (normalizando `dt` a `<dt>`, porque es la marca de ejecución) y el orden de campos. La salida real de la última ejecución queda en `output/` de cada ejemplo.

## Ejemplo principal (enunciado)

Entrada en `data/input/events/person/person.json`; esperados en `data/expected/STANDARD_OK.json` y `data/expected/STANDARD_KO.json`:

- OK (1): Fran.
- KO (2): Xabier (`office` vacío) y Miguel (sin `age`), ambos con `dt` y su `arraycoderrorbyfield`.

## 01 · Validaciones múltiples y parametrizadas (`01-validaciones-multiples/`)

Fuente JSON `employees` con 4 campos validados a la vez: `name notEmpty`, `age notNull + minValue(18)`, `office notEmpty + inSet(RIO,MAD,BCN,VAL)` y `email matchesRegex(...)`.

- OK (2): Ana y Lucia.
- KO (4), pensados para ver cómo se acumulan los códigos en `arraycoderrorbyfield`:
  - Luis, 17 años → `age: [BELOW_MIN_VALUE]`
  - nombre vacío y email sin `@` → dos campos en el mismo registro
  - Marta sin `age` → `age: [NULL_NOT_ALLOWED, BELOW_MIN_VALUE]` (dos reglas sobre el mismo campo: las reglas numéricas también rechazan el nulo)
  - Pedro con `office = XXX` → `office: [VALUE_NOT_IN_ALLOWED_SET]`

## 02 · Fuente CSV y conversión de formato (`02-ordenes-csv/`)

Fuente **CSV** con cabecera (`orders.csv`), salida publicada en JSON: prueba que el formato de entrada/salida es solo metadatos. Validaciones: `customer notEmpty`, `total notNull + positive`, `status inSet(PAID,PENDING,CANCELLED)`.

- OK (2): pedidos 1001 y 1006.
- KO (4): total `0` y total negativo (`NOT_POSITIVE`), cliente vacío (`EMPTY_NOT_ALLOWED`) y estado `UNKNOWN` (`VALUE_NOT_IN_ALLOWED_SET`). Con `inferSchema`, `total` llega como número y los nulos de CSV se detectan igual que en JSON.

## 03 · Transformaciones genéricas encadenadas (`03-transformaciones-genericas/`)

Cadena `rename_fields` (`name → product_name`) → `drop_fields` (`internal_note` desaparece de la salida) → `validate_fields` (`price positive`, `stock notNull + nonNegative`) → `add_fields` en ambas ramas (`dt` y `sku_upper` = `upper(sku)`).

- OK (2): Keyboard y Headphones (ojo: `stock = 0` es válido, `nonNegative` lo acepta).
- KO (2): Mouse con precio negativo y Monitor sin `stock` (`[NULL_NOT_ALLOWED, NEGATIVE_NOT_ALLOWED]`).
