#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This is an example of working with very large data. There are about
700,000 unduplicated donors in this database of Illinois political
campaign contributions.

With such a large set of input data, we cannot store all the comparisons 
we need to make in memory. Instead, we will read the pairs on demand
from the MySQL database.

__Note:__ You will need to run `python mysql_init_db.py` 
before running this script. See the annotates source for 
[mysql_init_db.py](mysql_init_db.html)

For smaller datasets (<10,000), see our
[csv_example](csv_example.html)
"""
from __future__ import print_function

import os
import itertools
import time
import logging
import optparse
import locale
import pickle
import multiprocessing

import MySQLdb
import MySQLdb.cursors

import dedupe
import dedupe.backport


# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. Added
# for convenience.  To enable verbose output, run `python
# examples/mysql_example/mysql_example.py -v`
optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
(opts, args) = optp.parse_args()
log_level = logging.WARNING 
if opts.verbose :
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose >= 2:
        log_level = logging.DEBUG
logging.getLogger().setLevel(log_level)

# ## Setup
MYSQL_CNF = os.path.abspath('.') + '/mysql.cnf'

settings_file = 'settings'
training_file = 'training.json'

start_time = time.time()

# You'll need to copy `examples/mysql_example/mysql.cnf_LOCAL` to
# `examples/mysql_example/mysql.cnf` and fill in your mysql database
# information in `examples/mysql_example/mysql.cnf`

# We use Server Side cursors (SSDictCursor and SSCursor) to [avoid
# having to have enormous result sets in memory](http://stackoverflow.com/questions/1808150/how-to-efficiently-use-mysqldb-sscursor).
con = MySQLdb.connect(db='estudantes',
                      charset='utf8',
                      read_default_file = MYSQL_CNF, 
                      cursorclass=MySQLdb.cursors.SSDictCursor)
c = con.cursor()
c.execute("SET net_write_timeout = 3600")

con2 = MySQLdb.connect(db='estudantes',
                       charset='utf8',
                       read_default_file = MYSQL_CNF, 
                       cursorclass=MySQLdb.cursors.SSCursor)
c2 = con2.cursor()
c2.execute("SET net_write_timeout = 3600")

# Increase max GROUP_CONCAT() length. The ability to concatenate long
# strings is needed a few times down below.
c.execute("SET group_concat_max_len = 10192")

# We'll be using variations on this following select statement to pull
# in campaign donor info.
#
# We did a fair amount of preprocessing of the fields in
# `mysql_init_db.py`

MATRICULA_SELECT = "SELECT rec_no, nome, birth_date, sexo, m_name, p_name, definiciency, cartorio, certidao, livro, "\
          " folha, nis, sus, cpf, natural1, raca, ingress, email, telephone, address, "\
          " num, compl, bairro, cep, municipio, situacao, date_mov " \
        "from processed_estudantes"

# ## Training

if os.path.exists(settings_file):
    print('reading from ', settings_file)
    with open(settings_file, 'rb') as sf :
        deduper = dedupe.StaticDedupe(sf, num_cores=4)
else:

    # Define the fields dedupe will pay attention to
    #
    # The address, city, and zip fields are often missing, so we'll
    # tell dedupe that, and we'll learn a model that take that into
    # account
    fields = [{'field' : 'nome', 'type': 'String'},
              {'field' : 'birth_date', 'type': 'DateTime', 'has missing' : True},
              {'field' : 'sexo', 'type': 'String'},
              {'field' : 'm_name', 'type': 'String', 'has missing' : True}
            #   {'field' : 'p_name', 'type': 'String', 'has missing' : True}
            #   {'field' : 'definiciency', 'type': 'String', 'has missing' : True},
            #   {'field' : 'cartorio', 'type': 'String', 'has missing' : True},
            #   {'field' : 'certidao', 'type': 'String', 'has missing' : True},
            #   {'field' : 'livro', 'type': 'String', 'has missing' : True},
            #   {'field' : 'folha', 'type': 'String', 'has missing' : True},
            #   {'field' : 'nis', 'type': 'String', 'has missing' : True},
            #   {'field' : 'sus', 'type': 'String', 'has missing' : True},
            #   {'field' : 'cpf', 'type': 'String', 'has missing' : True},
            #   {'field' : 'natural1', 'type': 'String', 'has missing' : True},
            #   {'field' : 'raca', 'type': 'String', 'has missing' : True},
            #   {'field' : 'ingress', 'type': 'String', 'has missing' : True},
            #   {'field' : 'email', 'type': 'String', 'has missing' : True},
            #   {'field' : 'telephone', 'type': 'String', 'has missing' : True},
            #   {'field' : 'address', 'type': 'String', 'has missing' : True},
            #   {'field' : 'num', 'type': 'String', 'has missing' : True},
            #   {'field' : 'compl', 'type': 'String', 'has missing' : True},
            #   {'field' : 'bairro', 'type': 'String', 'has missing' : True},
            #   {'field' : 'cep', 'type': 'String', 'has missing' : True},
            #   {'field' : 'municipio', 'type': 'String', 'has missing' : True},
            #   {'field' : 'situacao', 'type': 'String', 'has missing' : True},
            #   {'field' : 'date_mov', 'type': 'String', 'has missing' : True}
            ]

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields, num_cores=4)

    # We will sample pairs from the entire donor table for training
    c.execute(MATRICULA_SELECT)
    temp_d = {i: row for i, row in enumerate(c)}

    deduper.sample(temp_d)
    del temp_d

    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    #
    # __Note:__ if you want to train from
    # scratch, delete the training_file
    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file) as tf :
            deduper.readTraining(tf)

    # ## Active learning

    print('starting active labeling...')
    # Starts the training loop. Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.

    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    dedupe.convenience.consoleLabel(deduper)
    # When finished, save our labeled, training pairs to disk
    with open(training_file, 'w') as tf:
        deduper.writeTraining(tf)

    # Notice our the argument here
    #
    # `recall` is the proportion of true dupes pairs that the learned
    # rules must cover. You may want to reduce this if your are making
    # too many blocks and too many comparisons.
    deduper.train(recall=0.90)

    with open(settings_file, 'wb') as sf:
        deduper.writeSettings(sf)

    # We can now remove some of the memory hobbing objects we used
    # for training
    deduper.cleanupTraining()

## Blocking

print('blocking...')

# To run blocking on such a large set of data, we create a separate table
# that contains blocking keys and record ids
print('creating blocking_map database')
c.execute("DROP TABLE IF EXISTS blocking_map")
c.execute("CREATE TABLE blocking_map "
          "(block_key VARCHAR(200), rec_no INTEGER) "
          "CHARACTER SET utf8 COLLATE utf8_unicode_ci")


# If dedupe learned a Index Predicate, we have to take a pass
# through the data and create indices.
print('creating inverted index')

for field in deduper.blocker.index_fields :
    c2.execute("SELECT DISTINCT {field} FROM processed_estudantes "
               "WHERE {field} IS NOT NULL".format(field = field))
    field_data = (row[0] for row in c2)
    deduper.blocker.index(field_data, field)

# Now we are ready to write our blocking map table by creating a
# generator that yields unique `(block_key, rec_no)` tuples.
print('writing blocking map')

c.execute(MATRICULA_SELECT)
full_data = ((row['rec_no'], row) for row in c)
b_data = deduper.blocker(full_data)

# MySQL has a hard limit on the size of a data object that can be
# passed to it.  To get around this, we chunk the blocked data in
# to groups of 30,000 blocks
step_size = 30000

# We will also speed up the writing by of blocking map by using 
# parallel database writers
def dbWriter(sql, rows) :
    conn = MySQLdb.connect(db='estudantes',
                           charset='utf8',
                           read_default_file = MYSQL_CNF) 

    cursor = conn.cursor()
    cursor.executemany(sql, rows)
    cursor.close()
    conn.commit()
    conn.close()

pool = dedupe.backport.Pool(processes=2)

done = False

while not done :
    chunks = (list(itertools.islice(b_data, step)) for step in [step_size]*100)

    
    results = []

    for chunk in chunks :
        results.append(pool.apply_async(dbWriter,
                                        ("INSERT INTO blocking_map VALUES (%s, %s)", 
                                         chunk)))

    for r in results :
        r.wait()

    if len(chunk) < step_size :
        done = True

pool.close()

# Free up memory by removing indices we don't need anymore
deduper.blocker.resetIndices()

# Remove blocks that contain only one record, sort by block key and
# donor, key and index blocking map.

# These steps, particularly the sorting will let us quickly create
# blocks of data for comparison
print('prepare blocking table. this will probably take a while ...')

logging.info("indexing block_key")
c.execute("ALTER TABLE blocking_map "
          "ADD UNIQUE INDEX (block_key, rec_no)")

c.execute("DROP TABLE IF EXISTS plural_key")
c.execute("DROP TABLE IF EXISTS plural_block")
c.execute("DROP TABLE IF EXISTS covered_blocks")
c.execute("DROP TABLE IF EXISTS smaller_coverage")

# Many block_keys will only form blocks that contain a single
# record. Since there are no comparisons possible within such a
# singleton block we can ignore them.
#
# Additionally, if more than one block_key forms identifical blocks
# we will only consider one of them.
logging.info("calculating plural_key")
c.execute("CREATE TABLE plural_key "
          "(block_key VARCHAR(200), "
          " block_id INTEGER UNSIGNED AUTO_INCREMENT, "
          " PRIMARY KEY (block_id)) "
          "(SELECT MIN(block_key) AS block_key FROM "
          " (SELECT block_key, "
          "  GROUP_CONCAT(rec_no ORDER BY rec_no) AS block "
          "  FROM blocking_map "
          "  GROUP BY block_key HAVING COUNT(*) > 1) AS blocks "
          " GROUP BY block)")

logging.info("creating block_key index")
c.execute("CREATE UNIQUE INDEX block_key_idx ON plural_key (block_key)")

logging.info("calculating plural_block")
c.execute("CREATE TABLE plural_block "
          "(SELECT block_id, rec_no "
          " FROM blocking_map INNER JOIN plural_key "
          " USING (block_key))")

logging.info("adding rec_no index and sorting index")
c.execute("ALTER TABLE plural_block "
          "ADD INDEX (rec_no), "
          "ADD UNIQUE INDEX (block_id, rec_no)")

# To use Kolb, et.al's Redundant Free Comparison scheme, we need to
# keep track of all the block_ids that are associated with a
# particular donor records. We'll use MySQL's GROUP_CONCAT function to
# do this. This function will truncate very long lists of associated
# ids, so the maximum string length to try to was increased just after the
# connection was initialized at the top of this file to try to avoid this.


logging.info("creating covered_blocks")
c.execute("CREATE TABLE covered_blocks "
          "(SELECT rec_no, "
          " GROUP_CONCAT(block_id ORDER BY block_id) AS sorted_ids "
          " FROM plural_block "
          " GROUP BY rec_no)")

c.execute("CREATE UNIQUE INDEX matriculax ON covered_blocks (rec_no)")

# In particular, for every block of records, we need to keep
# track of a donor records's associated block_ids that are SMALLER than
# the current block's id. Because we ordered the ids when we did the
# GROUP_CONCAT we can achieve this by using some string hacks.
logging.info("creating smaller_coverage")
c.execute("CREATE TABLE smaller_coverage "
          "(SELECT rec_no, block_id, "
          " TRIM(',' FROM SUBSTRING_INDEX(sorted_ids, block_id, 1)) AS smaller_ids "
          " FROM plural_block INNER JOIN covered_blocks "
          " USING (rec_no))")

con.commit()


## Clustering

def candidates_gen(result_set) :
    lset = set

    block_id = None
    records = []
    i = 0
    for row in result_set :
        if row['block_id'] != block_id :
            if records :
                yield records

            block_id = row['block_id']
            records = []
            i += 1

            if i % 10000 == 0 :
                print(i, "blocks")
                print(time.time() - start_time, "seconds")

        smaller_ids = row['smaller_ids']
        
        if smaller_ids :
            smaller_ids = lset(smaller_ids.split(','))
        else :
            smaller_ids = lset([])
            
        records.append((row['rec_no'], row, smaller_ids))

    if records :
        yield records

c.execute("SELECT rec_no, nome, birth_date, sexo, m_name, p_name, definiciency, cartorio, certidao, livro, "
          " folha, nis, sus, cpf, natural1, raca, ingress, email, telephone, address, "
          " num, compl, bairro, cep, municipio, situacao, date_mov, "
          "block_id, smaller_ids "
          "FROM smaller_coverage "
          "INNER JOIN processed_estudantes "
          "USING (rec_no) "
          "ORDER BY (block_id)")

print('clustering...')
clustered_dupes = deduper.matchBlocks(candidates_gen(c),
                                      threshold=0.5)

print("clustering done")
n_clusters = 0
for cluster, scores in clustered_dupes :
    cluster_id = cluster[0]
    c.execute('SELECT * FROM processed_estudantes WHERE rec_no = %s', (cluster_id, ))
    m1 = list(c)[0]
    for rec_no, score in zip(cluster, scores) :
        if score > 0.7 and cluster_id != rec_no:
            c.execute('SELECT * FROM processed_estudantes WHERE rec_no = %s', (rec_no, ))
            m2 = list(c)[0]
            if m1['matricula'] != m2['matricula']:
                n_clusters += 1
                print(m1['matricula'], m2['matricula'], score)
                print('\t', (m1['nome'], m1['birth_date'], m1['sexo'], m1['m_name'], m1['p_name']))
                print('\t', (m2['nome'], m2['birth_date'], m2['sexo'], m2['m_name'], m2['p_name']))

print('# duplicate sets')
print(n_clusters)

# Close our database connection
c.close()
con.close()

print('ran in', time.time() - start_time, 'seconds')
