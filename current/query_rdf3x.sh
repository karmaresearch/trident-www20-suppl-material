#!/bin/sh

INPUTDIR=$1
QUERIES=$2
OUTPUTDIR=$3
CMD=$4
PARAMS="6"
TIMEOUT=180m

if [ -d $OUTPUTDIR ]; then
	echo "Removing output directory $OUTPUTDIR ..."
	rm -rf $OUTPUTDIR
fi
mkdir -p $OUTPUTDIR

echo "Querying the data..."
for FILENAME in `ls $QUERIES`
do
echo "Querying query $QUERIES/$FILENAME"

sudo /cm/shared/package/utils/bin/drop_caches

timeout $TIMEOUT $CMD $INPUTDIR $QUERIES/$FILENAME $PARAMS 2> ${OUTPUTDIR}/logs_$FILENAME > ${OUTPUTDIR}/results_$FILENAME
if [[ "$?" == "124" ]]; then
    echo "TIMEOUT" >> ${OUTPUTDIR}/logs_${FILENAME} 
    #the results are invalid. Remove them                                   
    echo "There was a timeout. Removing results ..."
    rm ${OUTPUDIR}/results_$FILENAME
else
    gzip ${OUTPUTDIR}/results_$FILENAME
fi
done
