#!/bin/bash
for i in {1..100}
do
   echo "Ejecución número: $i"
   make test
done
