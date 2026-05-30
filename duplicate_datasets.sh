#!/bin/bash

SIZE=$1
DATASET=$2
CANTIDAD=$3

case $SIZE in
  S) PREFIX="Small" ;;
  M) PREFIX="Medium" ;;
  L) PREFIX="Large" ;;
esac

INPUT_DIR="$(dirname "$0")/input"

for i in $(seq 0 $((CANTIDAD - 1))); do
  cp "$INPUT_DIR/${DATASET}-${PREFIX}_accounts.csv" "$INPUT_DIR/accounts_${i}.csv"
  cp "$INPUT_DIR/${DATASET}-${PREFIX}_Trans.csv"    "$INPUT_DIR/transactions_${i}.csv"
done

echo "Creados $CANTIDAD pares de archivos (${DATASET}-${PREFIX}) en $INPUT_DIR"



  # ./duplicate_datasets.sh S HI 3
  # ./duplicate_datasets.sh M LI 5
  # ./duplicate_datasets.sh L HI 2