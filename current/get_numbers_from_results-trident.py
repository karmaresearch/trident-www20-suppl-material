#!/usr/bin/python3

import sys
import os
import math
import numpy
import gzip


print ("QUERY\tCOLD_RUNTIME\tAVG_WARM_RUNTIME\tCOLD_Q_RUNTIME\tAVG_WARM_Q_RUNTIME\tROWS\tMAXMEM_MB\tIO_BYTES")
for f in os.listdir(sys.argv[1]):
    if f.startswith("logs"):
        runtimes = []
        runtimeQO = []
        runtimeQE = []
        coldR = -1
        coldRQO = -1
        coldRQE = -1
        rows = -1
        maxmem = -1
        iobytes = -1
        for line in open(sys.argv[1] + "/" + f):
            if "Runtime total" in line:
                if coldR == -1:
                    coldR = float(line[66:-4])
                else:
                    runtimes.append(float(line[66:-4]))
            if "Runtime queryexec:" in line:
                if coldRQE == -1:
                    coldRQE = float(line[66:-4])
                else:
                    runtimeQE.append(float(line[66:-4]))
            if "Runtime queryopti:" in line:
                if coldRQO == -1:
                    coldRQO = float(line[66:-4])
                else:
                    runtimeQO.append(float(line[66:-4]))
            if "Max memory" in line and maxmem == -1:
                maxmem = line[64:-4]
            if rows == -1 and "# rows" in line:
                rows = int(line[56:])
            if "Process IO Read bytes" in line and iobytes == -1:
                # Note: the second run is the "cold" run, the first run is for the query results and IO bytes
                # after which the cache gets flushed.
                iobytes = line[72:-1]
                coldR = -1
                coldRQE = -1
                coldRQO = -1

        nameQuery = f[5:]

        if rows == -1:
            rf = open(sys.argv[1] + '/results_' + nameQuery)
            rows = 0
            for line in rf:
                if not "<empty result" in line:
                    rows = rows + 1

        print (nameQuery + "\t" + str(coldR) + "\t" + str(numpy.mean(runtimes)) + "\t" + str(coldRQE) + "\t" + str(numpy.mean(runtimeQE)) + "\t" + str(rows) + "\t" + str(maxmem) + "\t" + str(iobytes))
