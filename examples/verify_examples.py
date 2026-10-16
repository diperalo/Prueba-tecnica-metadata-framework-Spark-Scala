#!/usr/bin/env python3
"""Comprobacion automatica de los ficheros ejemplarizantes.

Tras ejecutar los dataflows (examples/run_examples.ps1), verifica para el
ejemplo del enunciado y para cada ejemplo adicional que:
  - el destino contiene UN fichero publicado <prefijo>_ok|ko_<yyyyMMdd_HHmm>.json
    (sin part-* ni _staging a la vista del consumidor),
  - existe su entrada de catalogo y su contador de registros cuadra,
  - el contenido coincide con expected_*.json, normalizando `dt`
    (marca de ejecucion) al marcador <dt>.

Uso: python examples\verify_examples.py
"""
import glob
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

CASES = [
    ("data/output/events/person", "data/expected/STANDARD_OK.json"),
    ("data/output/discards/person", "data/expected/STANDARD_KO.json"),
    ("examples/01-validaciones-multiples/output/events/employees",
     "examples/01-validaciones-multiples/expected/expected_ok.json"),
    ("examples/01-validaciones-multiples/output/discards/employees",
     "examples/01-validaciones-multiples/expected/expected_ko.json"),
    ("examples/02-ordenes-csv/output/events/orders",
     "examples/02-ordenes-csv/expected/expected_ok.json"),
    ("examples/02-ordenes-csv/output/discards/orders",
     "examples/02-ordenes-csv/expected/expected_ko.json"),
    ("examples/03-transformaciones-genericas/output/events/products",
     "examples/03-transformaciones-genericas/expected/expected_ok.json"),
    ("examples/03-transformaciones-genericas/output/discards/products",
     "examples/03-transformaciones-genericas/expected/expected_ko.json"),
]

NAME = re.compile(r"^[a-z0-9-]+_(ok|ko)_\d{8}_\d{4}\.json$")


def load_json_lines(path):
    """Filas normalizadas ordenables + orden de campos de cada fila."""
    rows = []
    for line in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            row = json.loads(line)  # conserva el orden de campos del fichero
            if "dt" in row:
                row["dt"] = "<dt>"
            rows.append((json.dumps(row, sort_keys=True, ensure_ascii=False), list(row.keys())))
    return sorted(rows)


def check(output_dir, expected_file):
    out_dir = ROOT / output_dir
    published = [p for p in glob.glob(str(out_dir / "*.json"))
                 if not p.endswith(".catalog.json")]
    assert len(published) == 1, f"{output_dir}: se esperaba 1 fichero publicado y hay {published}"
    published = pathlib.Path(published[0])
    assert NAME.match(published.name), f"Nombre no conforme al patron: {published.name}"
    leftovers = [p.name for p in out_dir.iterdir()
                 if "part-" in p.name or "_staging" in p.name]
    assert not leftovers, f"{output_dir}: restos de Spark visibles: {leftovers}"

    catalog = out_dir / (published.name[:-5] + ".catalog.json")
    assert catalog.is_file(), f"Falta la entrada de catalogo: {catalog}"
    expected = load_json_lines(ROOT / expected_file)
    entry = json.loads(catalog.read_text(encoding="utf-8"))
    assert entry["records"] == len(expected), \
        f"{catalog}: records={entry['records']} pero se esperaban {len(expected)}"

    actual = load_json_lines(published)
    assert [dump for dump, _ in actual] == [dump for dump, _ in expected], (
        f"Contenido distinto de {expected_file}\nESPERADO:\n" + "\n".join(expected)
        + "\nACTUAL:\n" + "\n".join(actual))
    assert [keys for _, keys in actual] == [keys for _, keys in expected], (
        f"Orden de campos distinto del esperado en {published.name}: "
        f"{[keys for _, keys in actual]} frente a {[keys for _, keys in expected]}")
    print(f"OK  {published.relative_to(ROOT)}  ({len(expected)} registros, catalogo y orden de campos verificados)")


def main():
    for output_dir, expected_file in CASES:
        check(output_dir, expected_file)
    print(f"\n{len(CASES)} salidas ejemplarizantes verificadas correctamente.")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
