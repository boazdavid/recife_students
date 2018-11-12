#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This is a setup script for mysql_example.  It downloads a zip file of
Illinois campaign contributions and loads them into a MySQL database
named 'contributions'.
 
__Note:__ You will need to run this script first before execuing
[mysql_example.py](mysql_example.html).
 
Tables created:
* raw_table - raw import of entire CSV file
* donors - all distinct donors based on name and address
* recipients - all distinct campaign contribution recipients
* contributions - contribution amounts tied to donor and recipients tables
"""

from __future__ import print_function

import os
import zipfile
import warnings

try:
    from urllib2 import urlopen  # Python2
except ImportError:
    from urllib.request import urlopen   # Python3

import MySQLdb

warnings.filterwarnings('ignore', category=MySQLdb.Warning)

csv_file = 'students_enrolled.csv'

conn = MySQLdb.connect(read_default_file = os.path.abspath('.') + '/mysql.cnf', 
                       local_infile = 1,
                       sql_mode="ALLOW_INVALID_DATES",
                       db='estudantes')
c = conn.cursor()

print('importing raw data from csv...')
c.execute("DROP TABLE IF EXISTS estudantes")
c.execute("DROP TABLE IF EXISTS processed_estudantes")
c.execute("CREATE TABLE estudantes "
          "(rec_no INT, ano INT, rpa INT, unidade VARCHAR(100), cod VARCHAR(35), "
          " inep VARCHAR(35), modalidade VARCHAR(36), ano_ensino VARCHAR(20), "
          " turma VARCHAR(15), turno VARCHAR(11),  matricula VARCHAR(10), nome VARCHAR(120), "
          " birth_date VARCHAR(12), sexo VARCHAR(23), m_name VARCHAR(120), p_name VARCHAR(120), "
          " definiciency VARCHAR(23), cartorio VARCHAR(23), certidao VARCHAR(23), livro VARCHAR(23), "
          " folha VARCHAR(23), nis VARCHAR(23), sus VARCHAR(23), cpf VARCHAR(23), natural1 VARCHAR(23), "
          " raca VARCHAR(23), ingress VARCHAR(23), email VARCHAR(35), telephone VARCHAR(23), address VARCHAR(123), "
          " num VARCHAR(23), compl VARCHAR(23), bairro VARCHAR(23), cep VARCHAR(23), municipio VARCHAR(23), "
          " situacao VARCHAR(23), date_mov VARCHAR(23))"
          "CHARACTER SET utf8 COLLATE utf8_unicode_ci")
conn.commit()

c.execute("LOAD DATA LOCAL INFILE %s INTO TABLE estudantes "
          "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' " 
          "IGNORE 1 LINES "
          "(rec_no, ano, rpa, unidade, cod, inep, modalidade, ano_ensino, turma, turno, matricula, nome, birth_date, "
          " sexo, m_name, p_name, definiciency, cartorio, certidao, livro, "
          " folha, nis, sus, cpf, natural1, raca, ingress, email, telephone, address, "
          " num, compl, bairro, cep, municipio, situacao, date_mov )",
          (csv_file,))
conn.commit()


print('creating indexes on estudantes')
c.execute("ALTER TABLE estudantes ADD PRIMARY KEY(rec_no)")
conn.commit()

print('nullifying empty strings in estudantes')
c.execute("UPDATE estudantes "
          "SET "
        "nome = CASE nome WHEN '' THEN NULL ELSE nome END, "
        "birth_date = CASE birth_date WHEN '' THEN NULL ELSE birth_date END, "
        "sexo = sexo, "
        "m_name = CASE m_name WHEN '' THEN NULL ELSE m_name END, "
        "p_name = CASE p_name WHEN '' THEN NULL ELSE p_name END, "
        "definiciency = CASE definiciency WHEN '' THEN NULL ELSE definiciency END, "
        "cartorio = CASE cartorio WHEN '' THEN NULL ELSE cartorio END, "
        "certidao = CASE certidao WHEN '' THEN NULL ELSE certidao END, "
        "livro = CASE livro WHEN '' THEN NULL ELSE livro END, "
        "folha = CASE folha WHEN '' THEN NULL ELSE folha END, "
        "nis = CASE nis WHEN '' THEN NULL ELSE nis END, "
        "sus = CASE sus WHEN '' THEN NULL ELSE sus END, "
        "cpf = CASE cpf WHEN '' THEN NULL ELSE cpf END, "
        "natural1 = CASE natural1 WHEN '' THEN NULL ELSE natural1 END, "
        "raca = CASE raca WHEN '' THEN NULL ELSE raca END, "
        "ingress = CASE ingress WHEN '' THEN NULL ELSE ingress END, "
        "email = CASE email WHEN '' THEN NULL ELSE email END, "
        "telephone = CASE telephone WHEN '' THEN NULL ELSE telephone END, "
        "address = CASE address WHEN '' THEN NULL ELSE address END, "
        "num = CASE num WHEN '' THEN NULL ELSE num END, "
        "compl = CASE compl WHEN '' THEN NULL ELSE compl END, "
        "bairro = CASE bairro WHEN '' THEN NULL ELSE bairro END, "
        "cep = CASE cep WHEN '' THEN NULL ELSE cep END, "
        "municipio = CASE municipio WHEN '' THEN NULL ELSE municipio END, "
        "situacao = CASE situacao WHEN '' THEN NULL ELSE situacao END, "
        "date_mov = CASE date_mov WHEN '' THEN NULL ELSE date_mov END ")
conn.commit()

c.execute("CREATE TABLE processed_estudantes AS " 
          "(SELECT rec_no, rpa, " 
          " LOWER(unidade) AS unidade, " 
          " LOWER(cod) AS cod, " 
          " LOWER(inep) AS inep, " 
          " LOWER(modalidade) AS modalidade, " 
          " LOWER(ano) AS ano, " 
          " LOWER(turma) AS turma, " 
          " LOWER(turno) AS turno, " 
          " LOWER(nome) AS nome, " 
          " LOWER(matricula) AS matricula, " 
          " LOWER(birth_date) AS birth_date, " 
          " LOWER(sexo) AS sexo ,"
          " LOWER(m_name) AS m_name, " 
          " LOWER(p_name) AS p_name, " 
          " LOWER(definiciency) AS definiciency, " 
          " LOWER(cartorio) AS cartorio, " 
          " LOWER(certidao) AS certidao, " 
          " LOWER(livro) AS livro, " 
          " LOWER(folha) AS folha, " 
          " LOWER(nis) AS nis, " 
          " LOWER(sus) AS sus ,"
          " LOWER(cpf) AS cpf, " 
          " LOWER(natural1) AS natural1, " 
          " LOWER(raca) AS raca, " 
          " LOWER(ingress) AS ingress, " 
          " LOWER(email) AS email, " 
          " LOWER(telephone) AS telephone, " 
          " LOWER(address) AS address, " 
          " LOWER(num) AS num, " 
          " LOWER(compl) AS compl, " 
          " LOWER(bairro) AS bairro, " 
          " LOWER(cep) AS cep, " 
          " LOWER(municipio) AS municipio, " 
          " LOWER(situacao) AS situacao, " 
          " LOWER(date_mov) AS date_mov " 
          " FROM estudantes)")
 
c.execute("CREATE INDEX matricula_idx ON processed_estudantes (rec_no)")

c.close()
conn.close()
print('done')

