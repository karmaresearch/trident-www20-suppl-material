#!/bin/sh

INPUTDIR=$1
QUERIES=$2
OUTPUTDIR=$3
QUERY=$4 #query or query_native
CMD=$5
EXTRAOPTS=$6
DBNAME=`basename $INPUTDIR`

if [ -d $OUTPUTDIR ]; then
	echo "Removing output directory $OUTPUTDIR ..."
	rm -rf $OUTPUTDIR
fi
mkdir -p $OUTPUTDIR

echo "Querying the data..."
for FILENAME in `ls $QUERIES`
do
sudo /cm/shared/package/utils/bin/drop_caches
echo "Querying query $QUERIES/$FILENAME for getting results"
$CMD $QUERY $EXTRAOPTS -i ${INPUTDIR} -q $QUERIES/$FILENAME -l info > ${OUTPUTDIR}/logs_${FILENAME}_results 2> /dev/null
sudo /cm/shared/package/utils/bin/drop_caches
echo "Querying query $QUERIES/$FILENAME for stats"
$CMD $QUERY $EXTRAOPTS --decodeoutput false -i ${INPUTDIR} -q $QUERIES/$FILENAME -r 6 -l info > /dev/null 2> ${OUTPUTDIR}/logs_$FILENAME
done

echo "...done"
