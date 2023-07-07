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

cur.execute("""CREATE TABLE IF NOT EXISTS parcelles(
    id_parc SERIAL,
    id_ruche integer, 
    id_culture VARCHAR(4),
    Bio VARCHAR,
    annee integer,
    pourcentage_surface_butinage float, 
    PRIMARY KEY(id_parc),
    FOREIGN KEY(id_ruche) REFERENCES ruches(id_ruche),
    FOREIGN KEY(id_culture) REFERENCES cultures(id_culture))
    """)

print("Table parcelles créée.")

conn.commit()
cur.close()
conn.close()
