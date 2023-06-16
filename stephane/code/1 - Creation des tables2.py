import psycopg2
import sqlalchemy

#Connection à la base de données
DB_host='lucky.db.elephantsql.com'
DB_name='gsjpkacq'
DB_user='gsjpkacq'
DB_pass='HrEmIikKpM_k-CFk7Va_16W0CJuZ0c82'

conn=psycopg2.connect(dbname=DB_name, user=DB_user, password=DB_pass,host=DB_host)
cur=conn.cursor()

cur.execute("""CREATE TABLE cultures(
    ID_culture SERIAL, 
    nom VARCHAR, 
    debut_presence integer, 
    fin_presence integer, 
    debut_floraison integer, 
    fin_floraison integer, 
    debut_traitement integer,
    fin_traitement integer,
    PRIMARY KEY(ID_culture))
    """)
    
cur.execute("""CREATE TABLE parcelles(
    id_p SERIAL,
    ID_ruche VARCHAR, 
    ID_culture integer, 
    %_surface integer, 
    année integer,
    PRIMARY KEY(ID_p),
    FOREIGN KEY(ID_ruche) REFERENCES ruches(ID_ruche),
    FOREIGN KEY(ID_culture) REFERENCES cultures(ID_culture))""")
    
conn.commit()

cur.close()
conn.close()
