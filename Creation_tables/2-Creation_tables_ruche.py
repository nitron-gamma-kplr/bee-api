import psycopg2
from csv import reader
import pandas as pd

#Connection à la base de données

DB_host='lucky.db.elephantsql.com'
DB_name='gsjpkacq'
DB_user='gsjpkacq'
DB_pass='HrEmIikKpM_k-CFk7Va_16W0CJuZ0c82'

conn=psycopg2.connect(dbname=DB_name, user=DB_user, password=DB_pass,host=DB_host)
cur=conn.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS ruches(
    id_ruche SERIAL,
    api_id integer,
    nom_ruche VARCHAR UNIQUE, 
    zipc float, 
    commune VARCHAR, 
    adresse VARCHAR, 
    num integer, 
    latitude float,
    longitude float,
    x float, 
    y float,
    PRIMARY KEY(id_ruche),
    FOREIGN KEY(api_id) REFERENCES apiculteurs(api_id))
    """)
print("Table ruchers créée.")    


conn.commit()
cur.close()
conn.close()
