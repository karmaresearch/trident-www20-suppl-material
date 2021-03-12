#!/usr/bin/python

import sys
import os
import math
import numpy
import gzip

print ("QUERY\tCOLD_RUNTIME\tAVG_WARM_RUNTIME\tCOLD_Q_RUNTIME\tAVG_WARM_Q_RUNTIME\tROWS\tMAXMEM_MB\tIO_BYTES")
for f in os.listdir(sys.argv[1]):
    if f.startswith("logs"):
        runtimes = []
        runtimeQ = []
        coldR = -1
        coldRQ = -1
        rows = -1
        maxmem = -1
        iobytes = -1
        for line in open(sys.argv[1] + "/" + f):
            if "Time total" in line:
                if coldR == -1:
                    coldR = float(line[12:-4])
                else:
                    runtimes.append(float(line[12:-4]))
            if "Time query:" in line:
                if coldRQ == -1:
                    coldRQ = float(line[12:-4])
                else:
                    runtimeQ.append(float(line[12:-4]))
            if "Max mem" in line and maxmem == -1:
                maxmem = line[18:-1]
            if "IO Read bytes" in line and iobytes == -1:
                iobytes = line[14:-1]

        nameQuery = f[5:]
        rows = 0
        for line in gzip.open(sys.argv[1] + '/results_' + nameQuery + '.gz', 'rb'):
            if not b' cardinality' in line:
                rows = rows + 1
        print (nameQuery + "\t" + str(coldR) + "\t" + str(numpy.mean(runtimes)) + "\t" + str(coldRQ) + "\t" + str(numpy.mean(runtimeQ)) + "\t" + str(rows) + "\t" + str(maxmem) + "\t" + str(iobytes))
