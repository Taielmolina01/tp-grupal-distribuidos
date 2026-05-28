#!/bin/bash

SIZE=$1
CANTIDAD=$2

case $SIZE in
  S) PREFIX="Small" ;;
  M) PREFIX="Medium" ;;
  L) PREFIX="Large" ;;
esac

INPUT_DIR="$(dirname "$0")/input"

for i in $(seq 0 $((CANTIDAD - 1))); do
  cp "$INPUT_DIR/HI-${PREFIX}_accounts.csv" "$INPUT_DIR/accounts_${i}.csv"
  cp "$INPUT_DIR/HI-${PREFIX}_Trans.csv"    "$INPUT_DIR/transactions_${i}.csv"
done

echo "Creados $CANTIDAD pares de archivos ($PREFIX) en $INPUT_DIR"



  # ./duplicate_datasets.sh S 3   
  # ./duplicate_datasets.sh M 5   
  # ./duplicate_datasets.sh L 2   