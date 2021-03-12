import sys
import numpy as np
from os import listdir
from astropy.table import Table
from astropy.io import ascii

resultsdir = sys.argv[1]
tests = sys.argv[2]
fmt = sys.argv[3]


def gettime(line, startSubStr, endSubstr):
    pos = line.find(startSubStr)
    posEnd = line.find(endSubstr)
    number = line[pos + len(startSubStr):posEnd]
    return float(number)


def print_test1(resultsdir, fmt):
    results = {}
    confs = set()
    for perm in range(6):
        results[perm] = {}
        for typ in range(5):
            results[perm][typ] = {}
            for f in listdir(resultsdir + '/results'):
                # get last part of the name
                pos = f.find('_')
                name = f[pos+1:]
                confs.add(name)
                for line in open(resultsdir + '/results/' + f, 'rt'):
                    if line.startswith('PERM'):
                        continue
                    tokens = line.split('\t')
                    if int(tokens[0]) == perm and int(tokens[1]) == typ:
                        # Get warm runtime
                        results[perm][typ][name] = float(tokens[5])
                        break
    columns = ['perm', 'type']
    perms = []
    types = []
    confs = [n for n in confs]
    lenconfs = len(confs)
    colconfs = []
    colconfs.append(perms)
    colconfs.append(types)
    for i in range(lenconfs):
        columns.append(confs[i])
        colconfs.append([])
    for perm in range(6):
        for typ in range(5):
            perms.append(perm)
            types.append(typ)
            for idxconf in range(lenconfs):
                c = confs[idxconf]
                v = results[perm][typ][c]
                colconfs[idxconf+2].append(v)
    table = Table(colconfs,
                  names=columns)
    ascii.write(table, sys.stdout, format=fmt)


def print_test2_support(inputdir, column, nqueries, dataset, queries, lineToCatch):
    # Get prefix suffix
    prefix = None
    suffix = None
    for f in listdir(inputdir):
        if not f.endswith('results') and not f.endswith('.gz') and str(nqueries-1) in f:
            prefix = f[:f.find(str(nqueries-1))]
            suffix = f[f.find(str(nqueries-1))+len(str(nqueries-1)):]
    for j in range(nqueries + 1):
        for f in listdir(inputdir):
            if f == prefix + str(j) + suffix:
                # Read the file. Get the runtimes
                runtimes = []
                for line in open(inputdir + '/' + f, 'rt'):
                    if lineToCatch in line:
                        time = gettime(line, ': ', 'ms.')
                        runtimes.append(time)
                if queries is not None:
                    queries.append(dataset + '-' + str(j))
                column.append(np.average(np.array(runtimes[1:]))) # all but the first


def print_test2(resultsdir, fmt):
    # Three columns. Trident native-rdf3x and original RDF3X
    datasets = ['lubm1', 'btc2012']
    nqueries = [ 14, 5 ]
    queries = []
    trident = []
    trident_native = []
    rdf3x = []
    for i in range(len(datasets)):
        dataset = datasets[i]
        # Trident
        inputdir = resultsdir + '/results'
        inputdir2 = inputdir + '/' + dataset
        print_test2_support(inputdir2, trident, nqueries[i], dataset, queries, 'Runtime queryopti')
        inputdir2 = inputdir + '/' + dataset + '-native'
        print_test2_support(inputdir2, trident_native, nqueries[i], dataset, None, 'Runtime queryopti')
        # RDF3X
        inputdir = resultsdir + '/results' + dataset + '-rdf3x'
        print_test2_support(inputdir, rdf3x, nqueries[i], dataset, None, 'Time optimizer')

    table = Table([queries, trident_native, trident, rdf3x],
                  names=['Queries', 'Trident-ISO', 'Trident-RDF3X', 'RDF3X'])
    ascii.write(table, sys.stdout, format=fmt)


def print_test4(resultsdir, fmt):
    snapdir = resultsdir + '/snap'
    datasets = ['astro', 'web', 'twitter']
    snaplines = { 'hits' : 'Hits', 'pagerank' : 'PR', 'clustcoef' : 'ClusterCoeff',
                 'triangles' : 'Triangles', 'diameter' : 'Diameter',
                 'maxwcc' : 'MaxWcc', 'maxscc' : 'MaxScc',
                 'rw' : 'RandomWalk', 'bfs' : 'BFS',
                 'mod' : 'Mod', 'betcentr' : 'BetCentr'}
    inverseSnap = []
    resultsSnap = {}
    for k,v in snaplines.items():
        inverseSnap.append((v,k))
    for dataset in datasets:
        resultsSnap[dataset] = {}
        fileToUse = None
        for f in listdir(snapdir):
            if f.endswith(dataset):
                fileToUse = f
                break
        if fileToUse is None:
            continue
        # Process the file
        for line in open(snapdir + '/' + fileToUse):
            found = False
            for p in inverseSnap:
                if p[0] in line:
                    found = p[1]
                    break
            if found is not False:
                resultsSnap[dataset][found] = gettime(line, ': ', ' ms.')

    tridentdir = resultsdir + '/trident'
    resultsTrident = {}
    tasks = [k for k in snaplines.keys()]
    for dataset in datasets:
        resultsTrident[dataset] = {}
        for task in tasks:
            fileToUse = None
            for f in listdir(tridentdir):
                if f.startswith(dataset) and f.endswith(task):
                    fileToUse = f
                    break
            if fileToUse is None:
                continue
            for line in open(tridentdir + '/' + fileToUse):
                if 'Runtime' in line:
                    runtime = gettime(line, ': ', ' ms.')
                    resultsTrident[dataset][task] = runtime

    t_snaps = []
    t_trident = []
    for dataset in datasets:
        sr = []
        tr = []
        for t in tasks:
            if t in resultsSnap[dataset]:
                sr.append(resultsSnap[dataset][t])
            else:
                sr.append(-1)
            if t in resultsTrident[dataset]:
                tr.append(resultsTrident[dataset][t])
            else:
                tr.append(-1)
        t_snaps.append(sr)
        t_trident.append(tr)
    table = Table([tasks, t_snaps[0], t_trident[0], t_snaps[1], t_trident[1], t_snaps[2], t_trident[2]],
                  names=['task', 'snap(a)', 'trident(a)', 'snap(w)', 'trident(w)', 'snap(t)', 'trident(t)'])
    ascii.write(table, sys.stdout, format=fmt)



if tests == 'all' or tests == '1':
    print_test1(resultsdir + '/test1', fmt)

if tests == 'all' or tests == '2':
    print_test2(resultsdir + '/test2', fmt)

if tests == 'all' or tests == '4':
    print_test4(resultsdir + '/test4', fmt)
