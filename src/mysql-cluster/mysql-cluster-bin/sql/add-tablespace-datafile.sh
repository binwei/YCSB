#!/bin/bash

tablespace=$1
count=$2
initial_size=$3

for i in `seq 2 $count`;
do
datafile=$tablespace"_"data$i.dat
echo "alter tablespace $tablespace add datafile '$datafile' initial_size $initial_size engine ndbcluster;"
done