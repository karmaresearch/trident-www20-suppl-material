#!/bin/sh

INPUT_FILE=$1
OUTPUT_DIR=$2
CMD=$3
EXTRAOPTS=$4

PARAMS="$EXTRAOPTS  -i ${OUTPUT_DIR}/outputdb -f ${INPUT_FILE} -l debug"

if [ -d $OUTPUT_DIR ]; then
	echo "Removing output directory $OUTPUT_DIR ..."
	rm -rf $OUTPUT_DIR
fi
mkdir -p $OUTPUT_DIR

#Compress it
echo "Compressing the data..."
date >> ${OUTPUT_DIR}/comp_logs
echo $CMD load $PARAMS > ${OUTPUT_DIR}/comp_logs
$CMD load $PARAMS &>> ${OUTPUT_DIR}/comp_logs 
date >> ${OUTPUT_DIR}/comp_logs
echo "...done"
