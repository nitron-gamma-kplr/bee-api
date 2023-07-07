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

cur.execute("""CREATE TABLE IF NOT EXISTS time_series(
    id_value SERIAL,
    id_ruche integer, 
    GDH Timestamp, 
    Poids float, 
    PRIMARY KEY (id_value),
    FOREIGN KEY(id_ruche) REFERENCES ruches(id_ruche))
    """)

print("Table time_series créée.")

conn.commit()
cur.close()
conn.close()
