#!/usr/bin/python

import sys
import os
import math
import numpy
import gzip


print "QUERY\tCOLD_RUNTIME_MS\tAVG_WARM_RUNTIME_MS\tCOLD_Q_RUNTIME_MS\tAVG_WARM_Q_RUNTIME_MS\tROWS\tMAXMEM_MB\tIO_BYTES"
for f in os.listdir(sys.argv[1]):
    if not f.endswith("results"):
        runtimes = []
        runtimeQ = []
        coldR = 0
        coldRQ = 0
        rows = -1
        maxmem = -1
        iobytes = -1
        for line in open(sys.argv[1] + "/" + f):
            if "Runtime totalexec:" in line:
                if coldR == 0:
                    coldR = float(line[62:-4])
                else:
                    runtimes.append(float(line[62:-4]))
            if "Runtime queryexec:" in line:
                if coldRQ == 0:
                    coldRQ = float(line[62:-4])
                else:
                    runtimeQ.append(float(line[62:-4]))
            if "Max memory" in line and maxmem == -1:
                maxmem = line[60:-4]
            if "Process IO Read bytes" in line and iobytes == -1:
                iobytes = line[62:-1]

        nameQuery = f[5:]
        rf = open(sys.argv[1] + '/logs_' + nameQuery + '_results')
        rows = 0
        for line in rf:
            rows = rows + 1
        print f + "\t" + str(coldR) + "\t" + str(round(numpy.mean(runtimes),3)) + "\t" + str(round(coldRQ,3)) + "\t" + str(round(numpy.mean(runtimeQ),3)) + "\t" + str(rows) + "\t" + str(maxmem) + "\t" + str(iobytes)
