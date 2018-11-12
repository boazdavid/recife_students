# recife-students

## Setup
anaconda
https://www.anaconda.com/download/#windows

pip install -r requirements.txt

mkdir -p /usr/local/lib   
ln -s /usr/local/mysql/lib/libmysql* /usr/local/lib

mysql -u root -p
mysql_example davidbo$ python mysql_init_db.py mysql_example davidbo$ pip install -r requirements.txt
rotema-mac:mysql_example davidbo$ export DYLD_LIBRARY_PATH=/usr/local/mysql/lib:$DYLD_LIBRARY_PATH
which mysql_config
export PATH=$PATH:/usr/local/mysql/bin
mysql> CREATE DATABASE  estudantes;

mysql -u root -p --local-infile estudantes;

SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = ON;

USE estudantes;
select * from estudantes;

\q

## init DB
`python init_db.py`

reads file `students_enrolled_2018_1023.csv` and load the data to MYSQL server in database `estudantes`. It creates 2 tables: `estudantes`, `processed_estudantes`.

## train_cluster
`python train_cluster.py`

Compares and find duplicate records. It assumes that the `processed_estudantes` table is in the database.
comparison according to the following 4 fields:
```
nome
birth_date
sexo
m_name
```

This is done in 2 steps:
### Active training
If there is no `settings` and `training.json` files - it will starts a series of questions. In each question, two examples (student records) are displayed. The user has to respond `y`- it is a duplicate, or `n` no. As more training examples - the better. If you want to retrain again, just remove (or rename) the files.
Note, a new training is required in the case you rename fields, or add change the compared fields.

### Dedupe
In this stage, the program will use the "active training" examples from the previous step. 
Then, It will cluster similar records togetehr. Each cluster gets a score (0 to 1) of the similarity between the records.
Finally, the program write the clusters with score > 0.7 to the console output.

