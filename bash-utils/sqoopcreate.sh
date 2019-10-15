#!/bin/bash

# INFO : importacion tabla MySQL a HDFS

sqoop job \
--create importDiccionario \
-- import \
--connect "jdbc:mysql://formacion01.pragsis.local/diccionario" \
--username alumno \
--password Temporal1! \
--table medidas \
--m 1 \
--target-dir /user/master/grupo1/dictionary_data/ \
--split-by cod_var
