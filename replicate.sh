#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "uso: $0 <input> <output_prefix> <M> [N]"
    echo "  input:         archivo de origen"
    echo "  output_prefix: prefijo para los archivos de salida (ej: out -> out_1.csv, out_2.csv, ...)"
    echo "  M:             cantidad de copias a generar"
    echo "  N:             (opcional) cantidad de líneas a copiar por archivo"
    exit 1
    # Ej: ./replicate.sh datasets/accounts-FULL.csv datasets/accounts 4 3
}

[ $# -lt 3 ] && usage

INPUT="$1"
PREFIX="$2"
M="$3"
N="${4:-}"

if ! [[ "$M" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: M debe ser un entero positivo" >&2
    exit 1
fi

if [ ! -f "$INPUT" ]; then
    echo "error: archivo de input '$INPUT' no encontrado" >&2
    exit 1
fi

# Trim una sola vez si N está definido, luego copiar
SOURCE="$INPUT"
TMPFILE=""
if [ -n "$N" ]; then
    TMPFILE="$(mktemp)"
    head -n "$N" "$INPUT" > "$TMPFILE"
    SOURCE="$TMPFILE"
fi

for i in $(seq 0 "$M"); do
    cp "$SOURCE" "${PREFIX}_${i}.csv"
    echo "generado: ${PREFIX}_${i}.csv"
done

[ -n "$TMPFILE" ] && rm -f "$TMPFILE"
