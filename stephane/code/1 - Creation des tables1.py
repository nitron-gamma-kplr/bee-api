import psycopg2
import sqlalchemy

#Connection à la base de données
DB_host='lucky.db.elephantsql.com'
DB_name='gsjpkacq'
DB_user='gsjpkacq'
DB_pass='HrEmIikKpM_k-CFk7Va_16W0CJuZ0c82'

conn=psycopg2.connect(dbname=DB_name, user=DB_user, password=DB_pass,host=DB_host)
cur=conn.cursor()

cur.execute("""CREATE TABLE apiculteurs(
    ID_apiculteur VARCHAR, 
    Status VARCHAR, 
    zipc float, 
    commune VARCHAR, 
    adresse VARCHAR, 
    num integer, 
    latitude float,
    longitude float,
    x float, 
    y float,
    PRIMARY KEY(ID_apiculteur))
    """)
    
cur.execute("""CREATE TABLE ruches(
    id_r SERIAL,
    ID_ruche VARCHAR, 
    ID_apiculteur VARCHAR, 
    zipc float, 
    commune VARCHAR, 
    adresse VARCHAR, 
    num integer, 
    latitude float,
    longitude float,
    x float, 
    y float,
    PRIMARY KEY(ID_ruche),
    FOREIGN KEY(ID_apiculteur) REFERENCES apiculteurs(ID_apiculteur))""")
    
cur.execute("""CREATE TABLE time_series(
    ID_ruche VARCHAR, 
    GDH Timestamp, 
    Poids float, 
    FOREIGN KEY(ID_ruche) REFERENCES ruches(ID_ruche))""")

conn.commit()

cur.close()
conn.close()
