#!/usr/bin/python

import sys
import os
import shutil
import subprocess
import configparser
import shlex
from datetime import datetime

# Launch all experiments reported in the paper
if len(sys.argv) < 2:
    print("\n*** Script to launch the experiments for the paper ***\n")
    print("Usage: <path config file> <type of test> (the last param is optional, default is 'all')")
    print("       Test '1': Execute all possible scans on a given datasets.")
    print("       Test '2': Execute simple SPARQL queries.")
    print("       Test '3': Execute more complex SPARQL queries (BSBM benchmark).")
    print("       Test '4': Execute other graph analytics algorithms.")
    print("       Test '5': Execute the same queries over larger databases.")
    print("       Test '6': Execute graph analytics over the largest publicly available graph.")
    print("       Test '7': Execute additions/removals, and perform simple SPARQL queries on the result, lubm1b")
    print("       Test '8': Execute additions/removals, and perform simple SPARQL queries on the result, lubm125m")
    print("       Test '9': Execute additions/removals, and perform simple SPARQL queries on the result, wikidata")
    print("")
    print("Examples: './launch_experiments.py experiments.config 1' will execute test 1 using the inputs and queries provided in the config file")
    print("          './launch_experiments.py experiments.config all' will execute all tests using the inputs and queries provided in the config file")
    print("")
    exit()

config = configparser.ConfigParser()
config.read(sys.argv[1])
config = config['DEFAULT']

datasets_dir = config['input']
queries_dir = config['queries']
results_dir = config['output']
extraopts = config['extraopts']
trident_exec = config['trident']
rdf3xload_exec = config['rdf3xload']
rdf3xquery_exec = config['rdf3xquery']
rdf3xtest_exec = config['rdf3xtest']
testsnap_exec = config['testsnap']

test5_extraopts = config['test5_extraopts']
test6_extraopts = config['test6_extraopts']

test = 'all'
if len(sys.argv) > 2:
    test = sys.argv[2]

# generate string for results directory
def getResultsName():
    now = datetime.now() # current date and time
    return now.strftime("results-%d-%m-%y-%H:%M:%S")

# Load a Trident DB
def load_db(inputdir, outputdir, extraopts):
    print(" Loading db into " + outputdir + "...")
    cmd = trident_exec + ' load -l debug -f ' + inputdir + ' -i ' + outputdir + ' --logfile ' + outputdir + '.log ' + extraopts
    process = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
    process.wait()
    return process.returncode

# Add to a Trident DB
def add_db(inputdir, outputdir, extraopts, addition):
    print(" Adding to db into " + outputdir + "...")
    cmd = trident_exec + ' add -l debug --update ' + inputdir + ' -i ' + outputdir + ' --logfile ' + outputdir + '.add.log.' + addition + ' ' + extraopts
    process = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
    process.wait()
    return process.returncode

# Remove from a Trident DB
def rm_db(inputdir, outputdir, extraopts, removal):
    print(" Removing from db into " + outputdir + "...")
    cmd = trident_exec + ' rm -l debug --update ' + inputdir + ' -i ' + outputdir + ' --logfile ' + outputdir + '.rm.log.' + removal + ' ' + extraopts
    process = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
    process.wait()
    return process.returncode

# Merge updates in a Trident DB
def merge_db(outputdir, extraopts,merge):
    print(" Merging db into " + outputdir + "...")
    cmd = trident_exec + ' merge -l debug -i ' + outputdir + ' --logfile ' + outputdir + '.merge.log.' + merge + ' ' + extraopts
    process = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
    process.wait()
    return process.returncode

# Load a RDF3X DB
def load_db_rdf3x(inputdir, outputdir):
    print(" Loading RDF3x db into " + outputdir + "...")
    cmd = 'current/importdata_rdf3x.sh ' + inputdir + ' ' + outputdir + ' ' + rdf3xload_exec
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    process.wait()
    return process.returncode

def flushCache():
    process = subprocess.Popen(shlex.split('sudo /cm/shared/package/utils/bin/drop_caches'), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    process.wait()


# Launch a query with Trident
def query(db, queries, output, extraopts, query_type):
    print(" Launch queries on " + db)
    if os.path.exists(output):
        print(' Removing dir ' + output)
        shutil.rmtree(output)
    os.makedirs(output)
    fqueries = [ q for q in os.listdir(queries) if not q.startswith('.')]
    fqueries.sort()
    for queryname in fqueries:
        flushCache()
        print(' Querying ' + queryname + ' for getting results...')
        cmd = trident_exec + ' ' + query_type + ' -i ' + db + ' -q ' + queries + '/' + queryname + ' -l debug ' + extraopts
        fout = output + "/results_" + queryname
        o = open(fout, 'wt')
        ferr = output + "/logs_" + queryname
        o2 = open(ferr, 'wt')
        o2.write('CMD: ' + cmd + '\n')
        o2.flush()
        process = subprocess.Popen(shlex.split(cmd), stdout=o, stderr=o2)
        process.wait()
        flushCache()
        print(' Querying ' + queryname + ' for getting stats...')
        cmd = trident_exec + ' ' + query_type + ' -i ' + db + ' -q ' + queries + '/' + queryname + ' -l info  -r 6 --decodeoutput false' + extraopts
        o2.write('CMD: ' + cmd + '\n')
        o2.flush()
        process = subprocess.Popen(shlex.split(cmd), stderr=o2, stdout=subprocess.DEVNULL)
        process.wait()
    return process.returncode


# Launch a query with RDF3X
def query_rdf3x(db, queries, output):
    print(" Launch RDF3X queries on " + db)
    cmd = 'current/query_rdf3x.sh ' + db + ' ' + queries + ' ' + output + ' ' + rdf3xquery_exec
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    process.wait()
    return process.returncode


# Exec a generic command
def exec_cmd(cmd, o='', e=''):
    if o == '':
        o = subprocess.PIPE
    if e == '':
        e = subprocess.PIPE
    # Why was "shell=True" removed??? It is needed, and cannot be replaced with shlex.split, because then I/O redirection does not work.
    # Re-added shell=True. --Ceriel
    print("Command = " + cmd)
    process = subprocess.Popen(cmd, shell=True, stdout=o, stderr=e)
    process.wait()
    return process.returncode


def test1(inputdir, outputdir):
    outputdir = outputdir + "/test1"
    dbdir = outputdir + "/db"
    create_dbs = True
    # Does it already exist?
    if os.path.exists(dbdir):
        create_dbs = False
    if create_dbs:
        # Load all copies of yago2s
        os.makedirs(dbdir)
        # I need the mappings for RDF3X
        outputdb = dbdir + '/yago_mappings'
        load_db(inputdir + '/yago2s', outputdb, extraopts + ' --onlyCompress 1')
        # Load RDF3X database
        outputdb = dbdir + '/yago_rdf3x'
        print(" Loading db into " + outputdb + "...")
        cmd = rdf3xload_exec + ' ' + outputdb + ' ' +  dbdir + '/yago_mappings/triples.gz ' + dbdir + '/yago_mappings/dict.gz'
        exec_cmd(cmd)
        outputdb = dbdir + '/yago_default'
        load_db(inputdir + '/yago2s', outputdb, extraopts + ' --storeplainlist 1')
        outputdb = dbdir + '/yago_row'
        load_db(inputdir + '/yago2s', outputdb, extraopts + ' --enableFixedStrat 1 --fixedStrat 148')
        outputdb = dbdir + '/yago_cluster'
        load_db(inputdir + '/yago2s', outputdb, extraopts + ' --enableFixedStrat 1 --fixedStrat 181')
        outputdb = dbdir + '/yago_column'
        load_db(inputdir + '/yago2s', outputdb, extraopts + ' --enableFixedStrat 1 --fixedStrat 96')
        outputdb = dbdir + '/yago_aggr'
        load_db(inputdir + '/yago2s', outputdb, extraopts + ' --aggrIndices 1')
        outputdb = dbdir + '/yago_skipped'
        load_db(inputdir + '/yago2s', outputdb, extraopts + ' --skipTables 1')
        # Create the queries
        print(" Generating the queries...")
        cmd = trident_exec + ' testcq -i ' + dbdir + '/yago_default' + ' --testqueryfile ' + dbdir + "/queries"
        exec_cmd(cmd)
        # Shuffle them
        print(" Shuffling the queries...")
        cmd='sort -R ' + dbdir + "/queries > " + dbdir + "/queries_shuffled"
        exec_cmd(cmd)
    else:
        print(" Reuse the copy of the databases that are already existing")

    resultsdir = outputdir + "/" + getResultsName()
    os.makedirs(resultsdir)

    # Launch all the tests
    print(" Launch the test with RDF3x...")
    cmd = rdf3xtest_exec + ' ' + dbdir + '/yago_rdf3x ' + dbdir + '/queries_shuffled > ' + resultsdir + '/test_rdf3x'
    exec_cmd(cmd)
    print(" Launch the test default...")
    cmd = trident_exec + ' testti -l debug -i ' + dbdir + '/yago_default --testqueryfile ' + dbdir + '/queries_shuffled > ' + resultsdir + '/test_default'
    exec_cmd(cmd)
    print(" Launch the test row...")
    cmd = trident_exec + ' testti -l debug -i ' + dbdir + '/yago_row --testqueryfile ' + dbdir + '/queries_shuffled > ' + resultsdir + '/test_row'
    exec_cmd(cmd)
    #print(" Launch the test cluster...") -- takes too long
    #cmd = trident_exec + ' testti -l debug -i ' + dbdir + '/yago_cluster --testqueryfile ' + dbdir + '/queries_shuffled > ' + resultsdir + '/test_cluster'
    #exec_cmd(cmd)
    print(" Launch the test column...")
    cmd = trident_exec + ' testti -l debug -i ' + dbdir + '/yago_column --testqueryfile ' + dbdir + '/queries_shuffled > ' + resultsdir + '/test_column'
    exec_cmd(cmd)
    print(" Launch the test aggr...")
    cmd = trident_exec + ' testti -l debug -i ' + dbdir + '/yago_aggr --testqueryfile ' + dbdir + '/queries_shuffled > ' + resultsdir + '/test_aggr'
    exec_cmd(cmd)
    print(" Launch the test skipped...")
    cmd = trident_exec + ' testti -l debug -i ' + dbdir + '/yago_skipped --testqueryfile ' + dbdir + '/queries_shuffled > ' + resultsdir + '/test_skipped'
    exec_cmd(cmd)


def test2(inputdir, queriesdir, outputdir):
    # Load lubm1b, btc2012, uniprot, dbpedia, wikidata and launch the queries with trident (query and query_native), and with RDF3X
    outputdir = outputdir + "/test2"
    dbdir = outputdir + "/db"
    # Does it already exist?
    if not os.path.exists(dbdir):
        os.makedirs(dbdir)
    if not os.path.exists(dbdir + '/lubm1b'):
        outputdb = dbdir + '/lubm1b'
        load_db(inputdir + '/lubm1b', outputdb, extraopts)
    if not os.path.exists(dbdir + '/btc2012'):
        outputdb = dbdir + '/btc2012'
        load_db(inputdir + '/btc2012', outputdb, extraopts)
    if not os.path.exists(dbdir + '/uniprot'):
        outputdb = dbdir + '/uniprot'
        load_db(inputdir + '/uniprot', outputdb, extraopts)
    if not os.path.exists(dbdir + '/dbpedia'):
        outputdb = dbdir + '/dbpedia'
        load_db(inputdir + '/dbpedia', outputdb, extraopts)
    if not os.path.exists(dbdir + '/wikidata'):
        outputdb = dbdir + '/wikidata'
        load_db(inputdir + '/wikidata', outputdb, extraopts)
    if not os.path.exists(dbdir + '/lubm1b_rdf3x'):
        load_db_rdf3x(inputdir + '/lubm1b_raw', dbdir + '/lubm1b_rdf3x')
    if not os.path.exists(dbdir + '/btc2012_rdf3x'):
        load_db_rdf3x(inputdir + '/btc2012_raw', dbdir + '/btc2012_rdf3x')
    if not os.path.exists(dbdir + '/uniprot_rdf3x'):
        load_db_rdf3x(inputdir + '/uniprot/uniprot-sample.nt', dbdir + '/uniprot_rdf3x')
    if not os.path.exists(dbdir + '/dbpedia_rdf3x'):
        load_db_rdf3x(inputdir + '/dbpedia_raw_data', dbdir + '/dbpedia_rdf3x')
    if not os.path.exists(dbdir + '/wikidata_rdf3x'):
        load_db_rdf3x(inputdir + '/wikidata_raw', dbdir + '/wikidata_rdf3x')

    # Now the directory for sure exist
    resultsdir = outputdir + "/" + getResultsName()
    os.makedirs(resultsdir)

    # Launch all the tests
    print(" Launch the LUBM queries (native) ...")
    query(dbdir + '/lubm1b', queriesdir + '/lubm', resultsdir + '/lubm1b-native', extraopts, 'query_native')
    print(" Launch the LUBM queries  ...")
    query(dbdir + '/lubm1b', queriesdir + '/lubm', resultsdir + '/lubm1b', extraopts, 'query')
    print(" Launch the BTC2012 queries (native) ...")
    query(dbdir + '/btc2012', queriesdir + '/btc2012', resultsdir + '/btc2012-native', extraopts, 'query_native')
    print(" Launch the BTC2012 queries ...")
    query(dbdir + '/btc2012', queriesdir + '/btc2012', resultsdir + '/btc2012', extraopts, 'query')
    print(" Launch the Uniprot queries (native) ...")
    query(dbdir + '/uniprot', queriesdir + '/uniprot', resultsdir + '/uniprot-native', extraopts, 'query_native')
    print(" Launch the Uniprot queries  ...")
    query(dbdir + '/uniprot', queriesdir + '/uniprot', resultsdir + '/uniprot', extraopts, 'query')
    print(" Launch the DBpedia queries (native) ...")
    query(dbdir + '/dbpedia', queriesdir + '/dbpedia-examples', resultsdir + '/dbpedia-native', extraopts, 'query_native')
    print(" Launch the DBpedia queries ...")
    query(dbdir + '/dbpedia', queriesdir + '/dbpedia-examples', resultsdir + '/dbpedia', extraopts, 'query')
    print(" Launch the Wikidata queries (native) ...")
    query(dbdir + '/wikidata', queriesdir + '/wikidata', resultsdir + '/wikidata-native', extraopts, 'query_native')
    print(" Launch the Wikidata queries ...")
    query(dbdir + '/wikidata', queriesdir + '/wikidata', resultsdir + '/wikidata', extraopts, 'query')

    print(" Launch the LUBM queries with RDF3X ...")
    query_rdf3x(dbdir + '/lubm1b_rdf3x/outputdb', queriesdir + '/lubm', resultsdir + 'lubm1b-rdf3x')
    print(" Launch the BTC2012 queries with RDF3X ...")
    query_rdf3x(dbdir + '/btc2012_rdf3x/outputdb', queriesdir + '/btc2012', resultsdir + 'btc2012-rdf3x')
    print(" Launch the Uniprot queries with RDF3X ...")
    query_rdf3x(dbdir + '/uniprot_rdf3x/outputdb', queriesdir + '/uniprot', resultsdir + '/uniprot-rdf3x')
    print(" Launch the DBpedia queries with RDF3X ...")
    query_rdf3x(dbdir + '/dbpedia_rdf3x/outputdb', queriesdir + '/dbpedia-examples', resultsdir + '/dbpedia-rdf3x')
    print(" Launch the Wikidata queries with RDF3x ...")
    query_rdf3x(dbdir + '/wikidata_rdf3x/outputdb', queriesdir + '/wikidata', resultsdir + '/wikidata-rdf3x')

def test3(inputdir, queriesdir, outputdir):
    print("TODO")


def test4(inputdir, outputdir):
    # Astro (Collaboration network of Arxiv Astro Physics),
    # Webgraph google
    # twitter social circles (egoTwitter)

    # Create the trident databases if not existing
    outputdir = outputdir + "/test4"
    dbdir = outputdir + '/db'
    if not os.path.exists(dbdir): # Load the trident databases
        os.makedirs(dbdir)
        load_db(inputdir + '/snap-orig/astro/astro.gz', dbdir + '/astro', '--inputformat snap --flatTree 1')
        load_db(inputdir + '/snap-orig/web/web.gz', dbdir + '/web', '--inputformat snap --flatTree 1')
        load_db(inputdir + '/snap-orig/twitter/twitter.gz', dbdir + '/twitter', '--inputformat snap --flatTree 1')

    # Launch tests with SNAP
    print(" Launching test snap program...")
    resultsdir = outputdir + '/snap'
    if os.path.exists(resultsdir):
        print('Removing ' + resultsdir)
        shutil.rmtree(resultsdir)
    os.makedirs(resultsdir)
    dirdbs = ['astro', 'web', 'twitter']
    # dirdbs = ['astro']
    for i in range(len(dirdbs)):
        dirinput = inputdir + '/snap-orig/' + dirdbs[i] + '/'
        cmd = testsnap_exec + ' ' + dirinput + dirdbs[i] + '.gz ' + dirinput + 'output ' + dirinput + 'terms_snap ' + dirinput + 'terms_snap_p'
        fout = open(resultsdir + '/results_' + dirdbs[i], 'wt')
        ferr = open(resultsdir + '/results_' + dirdbs[i] + '-err', 'wt')
        exec_cmd(cmd, o=fout, e=ferr)
        fout.close()
        ferr.close()

    # Launch tests with Trident
    print(" Launching test trident program...")
    resultsdir = outputdir + '/trident'
    if os.path.exists(resultsdir):
        print('Removing ' + resultsdir)
        shutil.rmtree(resultsdir)
    os.makedirs(resultsdir)
    for i in range(len(dirdbs)):
        nodes = inputdir + '/snap-orig/' + dirdbs[i] + '/terms_trident'
        pairs = inputdir + '/snap-orig/' + dirdbs[i] + '/terms_trident_p'
        # operations = ['pagerank', 'hits', 'clustcoef', 'triangles', 'diameter', 'maxwcc', 'maxscc', 'rw', 'bfs', 'mod', 'betcentr']
        operations = ['pagerank', 'hits', 'clustcoef', 'triangles', 'diameter', 'maxwcc', 'maxscc', 'rw', 'bfs', 'mod']
        parameters = ['', '', '', '', 'testnodes=1000', '', '', 'len=3\;nodes=' + nodes, 'pairs=' + pairs, 'nodes=' + nodes, 'nodefrac=1.0']
        for j in range(len(operations)):
            oparg2 = ''
            if parameters[j] != '':
                oparg2 = ' --oparg2 ' + parameters[j]
            cmd = trident_exec + ' analytics -i ' + dbdir + '/' + dirdbs[i] + ' -l info ' + oparg2 + ' --op ' + operations[j]
            ferr = open(resultsdir + '/' + dirdbs[i] + '_' + operations[j], 'wt')
            fout = open(resultsdir + '/' + dirdbs[i] + '_' + operations[j] + '_stdout', 'wt')
            exec_cmd(cmd, o=fout, e=ferr)
            fout.close()
            ferr.close();


def test5(inputdir, queriesdir, outputdir):
    datasets = [ 1, 2, 5, 10, 20, 50, 100 ] # 1%, 2%, 20%, etc. of lubm10B
    outputdir = outputdir + "/test5"
    dbdir = outputdir + "/db/scale"
    # Load different LUBM databases of different sizes
    if not os.path.exists(dbdir):
        os.makedirs(dbdir)
    # inputdir = inputdir + '/lubm_800000'
    inputdir = inputdir + '/lubm10B'
    allfiles = [f for f in os.listdir(inputdir) if not f.startswith('.')]
    allfiles.sort()
    if len(allfiles) == 0:
        raise 'No files to load! Test aborted'

    for dataset in datasets:
        dir_db = dbdir + '/lubm_perc_' + str(dataset)
        if not os.path.exists(dir_db):
            # Create a temporary directory with a subset of the files
            tmpinput = outputdir + '/scale_inputdataset_' + str(dataset)
            if os.path.exists(tmpinput):
                print("Removing", tmpinput, '...')
                shutil.rmtree(tmpinput)
            print(' Creating', tmpinput)
            os.makedirs(tmpinput)
            filestocopy = int(len(allfiles) * dataset / 100)
            print(' Linking', filestocopy, ' files to the directory ', tmpinput)
            for i in range(filestocopy):
                os.symlink(inputdir + '/' + allfiles[i], tmpinput + '/' + allfiles[i])
            # Create the database
            load_db(tmpinput, dir_db, test5_extraopts)
            # Remove the directory
            print("Removing", tmpinput)
            shutil.rmtree(tmpinput)
    # Execute 5 LUBM queries on larger datasets
    resultsdir = outputdir + "/" + getResultsName()
    os.makedirs(resultsdir)
    # Launch the queries on the given dataset
    for dataset in datasets:
        dbdir2 = dbdir + '/lubm_perc_' + str(dataset)
        if not os.path.exists(resultsdir + '/lubm_native'):
            os.makedirs(resultsdir + '/lubm_native')
        query(dbdir2, queriesdir + '/lubm/', resultsdir + '/lubm_native/' + str(dataset), extraopts, 'query_native')
        if not os.path.exists(resultsdir + '/lubm'):
            os.makedirs(resultsdir + '/lubm')
        query(dbdir2, queriesdir + '/lubm/', resultsdir + '/lubm/' + str(dataset), extraopts, 'query')

def test6(inputdir, outputdir):
    dbdir = outputdir + "/db/"
    dbname = dbdir + '/hypergraph'
    if not os.path.exists(dbname):
        if not os.path.exists(dbdir):
            os.makedirs(dbdir)
        print(' Load database...')
        inputfiles = inputdir + '/hypergraph'
        cmd = trident_exec + ' load -l debug --comprinput ' + inputfiles + '/part-r-00 -i ' + dbname + ' --logfile ' + dbname + '.log ' + test6_extraopts
        process = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
        process.wait()
    print(' Launch PageRank...')
    outputdir = outputdir + '/test6'
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    cmd = trident_exec + ' analytics -i ' + dbname + ' -l info --op pagerank'
    fout = open(outputdir + '/pagerank.log', 'wt')
    process = subprocess.Popen(shlex.split(cmd), stderr=fout)
    process.wait()


# lubm1b_parts/part7950 consists of the bulk of lubm1b, everything except for the last 50  universities
# lubm1b_parts/partX-Y is Y-X universities, starting from UniversityX
def test7(inputdir, queriesdir, outputdir):
    addparts = [ '7950-7960', '7960-7970', '7970-7980', '7980-7990', '7990-8000' ]
    rmparts = [ '100-110', '200-210', '300-310', '400-410', '500-510' ]
    outputdir = outputdir + "/test7"
    dbdir = outputdir + "/db"
    # Does it already exist?
    if not os.path.exists(dbdir):
        os.makedirs(dbdir)
    outputdb = dbdir + '/lubm1b_updates'
    if not os.path.exists(outputdb):
        load_db(inputdir + '/lubm1b_parts/part7950', outputdb, extraopts)
    else:
        if os.path.exists(outputdb + '/_diff'):
            shutil.rmtree(outputdb + '/_diff')

    # Now the directory for sure exist
    resultsdir = outputdir + "/" + getResultsName()
    os.makedirs(resultsdir)

    print(" Launch the LUBM queries  ...")
    query(outputdb, queriesdir + '/lubm-test', resultsdir + '/lubm7950', extraopts, 'query')

    count = 0
    for addpart in addparts:
        count = count + 1
        print("Adding 10 universities")
        add_db(inputdir + '/lubm1b_parts/part' + addpart, outputdb, extraopts, addpart)

        print(" Launch the LUBM queries  ...")
        query(outputdb, queriesdir + '/lubm-test', resultsdir + '/parts-added-' + str(count), extraopts, 'query')

    print("Merging updates")
    merge_db(outputdb, extraopts, 'add')

    print(" Launch the LUBM queries  ...")
    query(outputdb, queriesdir + '/lubm-test', resultsdir + '/parts-merged-after-add', extraopts, 'query')

    count = 0
    for rmpart in rmparts:
        count = count + 1
        print("Removing 10 universities")
        rm_db(inputdir + '/lubm1b_parts/part' + rmpart, outputdb, extraopts, rmpart)

        print(" Launch the LUBM queries  ...")
        query(outputdb, queriesdir + '/lubm-test', resultsdir + '/parts-removed-' + str(count), extraopts, 'query')

    print("Merging updates")
    merge_db(outputdb, extraopts, 'db')

    print(" Launch the LUBM queries  ...")
    query(outputdb, queriesdir + '/lubm-test', resultsdir + '/parts-merged-after-rm', extraopts, 'query')

# Similar to test7, smaller dataset
def test8(inputdir, queriesdir, outputdir):
    addparts = [ '995', '996', '997', '998', '999' ]
    rmparts = [ '100', '200', '300', '400', '500' ]
    outputdir = outputdir + "/test8"
    dbdir = outputdir + "/db"
    # Does it already exist?
    if not os.path.exists(dbdir):
        os.makedirs(dbdir)
    outputdb = dbdir + '/lubm1000_updates'
    if not os.path.exists(outputdb):
        load_db(inputdir + '/lubm1000_parts/lubm995', outputdb, extraopts)
    else:
        if os.path.exists(outputdb + '/_diff'):
            shutil.rmtree(outputdb + '/_diff')

    # Now the directory for sure exist
    resultsdir = outputdir + "/" + getResultsName()
    os.makedirs(resultsdir)

    print(" Launch the LUBM queries  ...")
    query(outputdb, queriesdir + '/lubm-test', resultsdir + '/lubm995', extraopts, 'query')

    count = 0
    for addpart in addparts:
        count = count + 1
        print("Adding 1 university")
        add_db(inputdir + '/lubm1000_parts/lubm-u' + addpart, outputdb, extraopts, addpart)

        print(" Launch the LUBM queries  ...")
        query(outputdb, queriesdir + '/lubm-test', resultsdir + '/parts-added-' + str(count), extraopts, 'query')

    print("Merging updates")
    merge_db(outputdb, extraopts, 'add')

    print(" Launch the LUBM queries  ...")
    query(outputdb, queriesdir + '/lubm-test', resultsdir + '/parts-merged-after-add', extraopts, 'query')

    count = 0
    for rmpart in rmparts:
        count = count + 1
        print("Removing 1 universities")
        rm_db(inputdir + '/lubm1000_parts/lubm-u' + rmpart, outputdb, extraopts, rmpart)

        print(" Launch the LUBM queries  ...")
        query(outputdb, queriesdir + '/lubm-test', resultsdir + '/parts-removed-' + str(count), extraopts, 'query')

    print("Merging updates")
    merge_db(outputdb, extraopts, 'db')

    print(" Launch the LUBM queries  ...")
    query(outputdb, queriesdir + '/lubm-test', resultsdir + '/parts-merged-after-rm', extraopts, 'query')

# Similar to test7, but with wikidata
def test9(inputdir, queriesdir, outputdir):
    addparts = [ '54', '42', '14', '34', '61' ]
    rmparts = [ '55', '10', '49', '47', '60' ]
    outputdir = outputdir + "/test9"
    dbdir = outputdir + "/db"
    # Does it already exist?
    if not os.path.exists(dbdir):
        os.makedirs(dbdir)
    outputdb = dbdir + '/wikidata_updates'
    if not os.path.exists(outputdb):
        load_db(inputdir + '/wikidata.parts/db', outputdb, extraopts)
    else:
        if os.path.exists(outputdb + '/_diff'):
            shutil.rmtree(outputdb + '/_diff')

    # Now the directory for sure exist
    resultsdir = outputdir + "/" + getResultsName()
    os.makedirs(resultsdir)

    print(" Launch the Wikidata queries  ...")
    query(outputdb, queriesdir + '/wikidata', resultsdir + '/wikidata-', extraopts, 'query')

    count = 0
    for addpart in addparts:
        count = count + 1
        print("Adding 1MB facts")
        add_db(inputdir + '/wikidata.parts/head' + addpart, outputdb, extraopts, str(count))

        print(" Launch the Wikidata queries  ...")
        query(outputdb, queriesdir + '/wikidata', resultsdir + '/parts-added-' + str(count), extraopts, 'query')

    print("Merging updates")
    merge_db(outputdb, extraopts, 'add')

    print(" Launch the Wikidata queries  ...")
    query(outputdb, queriesdir + '/wikidata', resultsdir + '/parts-merged-after-add', extraopts, 'query')

    count = 0
    for rmpart in rmparts:
        count = count + 1
        print("Removing 1MB facts")
        rm_db(inputdir + '/wikidata.parts/head' + rmpart, outputdb, extraopts, str(count))

        print(" Launch the Wikidata queries  ...")
        query(outputdb, queriesdir + '/wikidata', resultsdir + '/parts-removed-' + str(count), extraopts, 'query')

    print("Merging updates")
    merge_db(outputdb, extraopts, 'db')

    print(" Launch the Wikidata queries  ...")
    query(outputdb, queriesdir + '/wikidata', resultsdir + '/parts-merged-after-rm', extraopts, 'query')

def test10(inputdir, queriesdir, outputdir):
    # Load lubm1b, btc2012, uniprot, dbpedia, wikidata and launch the queries with trident (query and query_native). This is like test2, but using the skipTables option.
    outputdir = outputdir + "/test10"
    dbdir = outputdir + "/db"
    # Does it already exist?
    if not os.path.exists(dbdir):
        os.makedirs(dbdir)
    if not os.path.exists(dbdir + '/lubm1b'):
        outputdb = dbdir + '/lubm1b'
        load_db(inputdir + '/lubm1b', outputdb, '--skipTables true')
    if not os.path.exists(dbdir + '/btc2012'):
        outputdb = dbdir + '/btc2012'
        load_db(inputdir + '/btc2012', outputdb, '--skipTables true')
    if not os.path.exists(dbdir + '/uniprot'):
        outputdb = dbdir + '/uniprot'
        load_db(inputdir + '/uniprot', outputdb, '--skipTables true')
    if not os.path.exists(dbdir + '/dbpedia'):
        outputdb = dbdir + '/dbpedia'
        load_db(inputdir + '/dbpedia', outputdb, '--skipTables true')
    if not os.path.exists(dbdir + '/wikidata'):
        outputdb = dbdir + '/wikidata'
        load_db(inputdir + '/wikidata', outputdb, '--skipTables true')

    # Now the directory for sure exist
    resultsdir = outputdir + "/" + getResultsName()
    os.makedirs(resultsdir)

    # Launch all the tests
    print(" Launch the LUBM queries (native) ...")
    query(dbdir + '/lubm1b', queriesdir + '/lubm', resultsdir + '/lubm1b-native', extraopts, 'query_native')
    print(" Launch the LUBM queries  ...")
    query(dbdir + '/lubm1b', queriesdir + '/lubm', resultsdir + '/lubm1b', extraopts, 'query')
    print(" Launch the BTC2012 queries (native) ...")
    query(dbdir + '/btc2012', queriesdir + '/btc2012', resultsdir + '/btc2012-native', extraopts, 'query_native')
    print(" Launch the BTC2012 queries ...")
    query(dbdir + '/btc2012', queriesdir + '/btc2012', resultsdir + '/btc2012', extraopts, 'query')
    print(" Launch the Uniprot queries (native) ...")
    query(dbdir + '/uniprot', queriesdir + '/uniprot', resultsdir + '/uniprot-native', extraopts, 'query_native')
    print(" Launch the Uniprot queries  ...")
    query(dbdir + '/uniprot', queriesdir + '/uniprot', resultsdir + '/uniprot', extraopts, 'query')
    print(" Launch the DBpedia queries (native) ...")
    query(dbdir + '/dbpedia', queriesdir + '/dbpedia-examples', resultsdir + '/dbpedia-native', extraopts, 'query_native')
    print(" Launch the DBpedia queries ...")
    query(dbdir + '/dbpedia', queriesdir + '/dbpedia-examples', resultsdir + '/dbpedia', extraopts, 'query')
    print(" Launch the Wikidata queries (native) ...")
    query(dbdir + '/wikidata', queriesdir + '/wikidata', resultsdir + '/wikidata-native', extraopts, 'query_native')
    print(" Launch the Wikidata queries ...")
    query(dbdir + '/wikidata', queriesdir + '/wikidata', resultsdir + '/wikidata', extraopts, 'query')

if test == 'all' or test == '1':
    print("*** Begin Test 1 ***")
    test1(datasets_dir, results_dir)
    print("*** End Test 1 ***")

if test == 'all' or test == '2':
    print("*** Begin Test 2 ***")
    test2(datasets_dir, queries_dir, results_dir)
    print("*** End Test 2 ***")

if test == 'all' or test == '3':
    print("*** Begin Test 3 ***")
    test3(datasets_dir, queries_dir, results_dir)
    print("*** End Test 3 ***")

if test == 'all' or test == '4':
    print("*** Begin Test 4 ***")
    test4(datasets_dir, results_dir)
    print("*** End Test 4 ***")

if test == 'all' or test == '5':
    print("*** Begin Test 5 ***")
    test5(datasets_dir, queries_dir, results_dir)
    print("*** End Test 5 ***")

if test == 'all' or test == '6':
    print("*** Begin Test 6 ***")
    test6(datasets_dir, results_dir)
    print("*** End Test 6 ***")

if test == 'all' or test == '7':
    print("*** Begin Test 7 ***")
    test7(datasets_dir, queries_dir, results_dir)
    print("*** End Test 7 ***")

if test == 'all' or test == '8':
    print("*** Begin Test 8 ***")
    test8(datasets_dir, queries_dir, results_dir)
    print("*** End Test 8 ***")

if test == 'all' or test == '9':
    print("*** Begin Test 9 ***")
    test9(datasets_dir, queries_dir, results_dir)
    print("*** End Test 9 ***")

if test == 'all' or test == '10':
    print("*** Begin Test 10 ***")
    test10(datasets_dir, queries_dir, results_dir)
    print("*** End Test 10 ***")
