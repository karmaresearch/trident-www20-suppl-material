#!/bin/sh

INPUT_FILE=$1
OUTPUT_DIR=$2
CMD=$3

if [ -d $OUTPUT_DIR ]; then
	echo "Removing output directory $OUTPUT_DIR ..."
	rm -rf $OUTPUT_DIR
fi
mkdir -p $OUTPUT_DIR

#Compress it
echo "Compressing the data..."
date >> ${OUTPUT_DIR}/comp_logs
$CMD ${OUTPUT_DIR}/outputdb ${INPUT_FILE} &>> ${OUTPUT_DIR}/comp_logs 
date >> ${OUTPUT_DIR}/comp_logs
echo "...done"
