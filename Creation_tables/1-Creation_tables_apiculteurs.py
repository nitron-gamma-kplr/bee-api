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

cur.execute("""CREATE TABLE IF NOT EXISTS apiculteurs(
    api_id INTEGER, 
    Status VARCHAR, 
    zipc float, 
    commune VARCHAR, 
    adresse VARCHAR, 
    num integer, 
    latitude float,
    longitude float,
    x float, 
    y float,
    PRIMARY KEY(api_id))
    """)

requete="INSERT INTO apiculteurs (api_id) VALUES ('0')"
cur.execute (requete)
conn.commit()
print("Table apiculteurs créée.")

conn.commit()
cur.close()
conn.close()
