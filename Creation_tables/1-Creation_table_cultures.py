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

cur.execute("""CREATE TABLE IF NOT EXISTS cultures(
    id_cult SERIAL,
    id_culture VARCHAR(4) UNIQUE,
    nom VARCHAR, 
    debut_presence integer, 
    fin_presence integer, 
    debut_floraison integer, 
    fin_floraison integer,
    debut_traitement integer,
    fin_traitement integer,
    PRIMARY KEY(id_culture))
    """)

print("Table cultures créée ; remplissage en cours à partir du fichier Datas\cultures\Codification_cultures.csv ...")
#remplissage liste culture

df=pd.read_csv('D:\FORMATION_DATASCIENCES\PROJET_BEE-API\Datas\cultures\Codification_cultures.csv',sep=";")
long = len(df)
for i in range(long):
    liste=list(df.iloc[i])
    id_culture=str(liste[0])
    nom=str(liste[1])
    nom=nom.replace("'","_")
    param="('"+id_culture+"','"+nom+"')"
    requete="INSERT INTO cultures (id_culture,nom) VALUES" + param + "ON CONFLICT (id_culture) DO NOTHING"
    cur.execute (requete)
    conn.commit()
print("Table cultures : fin de remplissage.")
cur.close()
conn.close()
