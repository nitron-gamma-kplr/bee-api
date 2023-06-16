import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import xlrd
import requests, json
import urllib.parse as up
import datetime
from datetime import datetime
import os
import sys
from hashlib import md5
from os import path
import psycopg2
import psycopg2.extras
import pyodbc


# verifie la presence de fichiers dans le dossier 'new_datas". Si non, quitte le programme et si oui renvoie une liste avce le nom des fichiers présents
def verif_files(directory): 
    list_files=os.listdir(directory)
    
    nd_files = len(list_files)
    if len(list_files) ==0 :
        print("Il n'y a rien à traiter dans le dossier '0_new_data'.")
        sys.exit()
    return(list_files)

# cree deux noms uniques pour la sauvegarde des données (métas et Ts) du fichier exploité
def cree_nom_fichier_avec_datetime(nom_fichier):
    nom_fichier=nom_fichier[0:len(nom_fichier)-4]
    d = datetime.now()
    f = d.strftime('%Y%m%d%H%M%S-')
    p=('PROJET - BEE HAPPY/Datas/1_exploited_data/', '.csv')
    new_name1 = p[0] + f + nom_fichier + '_MD' + p[1]
    new_name2 = p[0] + f + nom_fichier + '_TS' + p[1]
    new_name3 = p[0] + f + nom_fichier + '_AP' + p[1]
    return(new_name1,new_name2,new_name3)

#interroge la base de donnees adresses.gouv à partir de l'adresse de la ruche et renvoie les metadonnées
def extract_coord(adr):
    api_url = "https://api-adresse.data.gouv.fr/search/?q="
    params={'q': adr,'limit':2}
    r = requests.get(api_url, params=params )
    j = r.json()
    
    if len(j.get('features')) > 0:
        first_result = j.get('features')[0]
        longitude, latitude = first_result.get('geometry').get('coordinates')
        num = first_result.get('properties').get('housenumber')
        add = first_result.get('properties').get('street')
        zipc = first_result.get('properties').get('postcode')
        city = first_result.get('properties').get('city')
        x= first_result.get('properties').get('x')
        y= first_result.get('properties').get('y')
    else:
        print('No result')

    return(latitude, longitude, num, add, zipc, city,x,y)

#sauvegadre les données importées sous 3 fichiers .csv horodatés
def sauvegarde_fichiers_csv (df_ru, df_ts,df_api,name):
    nom_fichier=name
    new_name1, new_name2, new_name3=cree_nom_fichier_avec_datetime(nom_fichier)
    #sauvegarde metas
    df_ru.to_csv(new_name1,index=False)
        #sauvegarde time_series
    df_ts.to_csv(new_name2,index=False)
    #sauvegarde apiculteurs
    df_api.to_csv(new_name3,index=False)

#extrait les données d'un classeur
def extract_datas(name):
       
    data_input=xlrd.open_workbook("PROJET - BEE HAPPY/Datas/Ruches/"+ name)
    nb_feuilles=data_input.nsheets
    liste=[]
    liste2=[]
    liste3=[]
    
    for feuille_active in data_input:
        status=feuille_active.cell_value(0, 0)
        adresse_comp=feuille_active.cell_value(1, 0)
        latitude, longitude, num, add, zipc, city,x,y =extract_coord(adresse_comp)
        
        # création de l'ID
        z=bytes(str(x*y),'utf-8')
        ID=md5(z).hexdigest()
        #inserer la un module qui va chercher l'ID de l'apiculteur correspondant à la ruche. Pour l'instant on copie l'ID de la ruche
        ID_api=ID

        liste.append([ID,ID_api,zipc,city,add,num,latitude,longitude,x,y])
        liste3.append([ID_api,status,zipc,city,add,num,latitude,longitude,x,y])

        for r in range(3,feuille_active.nrows):
            val_courante=(feuille_active.cell_value(rowx=r, colx=0))
            GDH=xlrd.xldate_as_datetime(val_courante, 0)
            poids=feuille_active.cell_value(rowx=r, colx=1)
            if poids=="":
                poids=0
            liste2.append([ID,GDH,poids])
    return(liste,liste2,liste3)

# ecrit les données extraites dans la base de données
def inscription_DB(df_ru,df_ts,df_api):

    long = len(df_api)
    for i in range(long):
        liste2=list(df_api.iloc[i])
        param=str(liste2)
        param=" ("+param[1:-1]+")"
        requete="INSERT INTO apiculteurs (ID_apiculteur,Status,zipc,commune,adresse,num,latitude,longitude,x,y) VALUES" + param
        cur.execute (requete)
        conn.commit()

    #Inscriptions dans la table ruches
    long = len(df_ru)
    for i in range(long):
        liste2=list(df_ru.iloc[i])
        param=str(liste2)
        param=" ("+param[1:-1]+")"
        requete="INSERT INTO ruches (ID_ruche,ID_apiculteur,zipc,commune,adresse,num,latitude,longitude,x,y) VALUES" + param
        cur.execute (requete)
        conn.commit()

    #Inscriptions dans la table t-s
    long = len(df_ts)
    for i in range(long):
        liste2=list(df_ts.iloc[i])
        liste2[1]=str(liste2[1])
        param=str(liste2)
        param=" ("+param[1:-1]+")"
        requete="INSERT INTO time_series (ID_ruche,GDH,poids) VALUES" + param
        cur.execute (requete)
        conn.commit()

# A VOIR PLUS TARD
def trace_courbes(classeur):
    document = xlrd.open_workbook(classeur_xlsx)

    print("Nombre de feuilles: "+str(document.nsheets))
    print("Noms des feuilles: "+str(document.sheet_names()))

    feuille_1 = document.sheet_by_index(0)
    feuille_1 = document.sheet_by_name("Fonction 1")

    print("Format de la feuille 1:")
    print("Nom: "+str(feuille_1.name))
    print("Nombre de lignes: "+str(feuille_1.nrows))
    print("Nombre de colonnes: "+str(feuille_1.ncols))

    cols = feuille_1.ncols
    rows = feuille_1.nrows

    X = []
    Y= []

    for r in range(1, rows):
        X += [feuille_1.cell_value(rowx=r, colx=0)]
        Y += [feuille_1.cell_value(rowx=r, colx=1)]

    plt.plot(X, Y)
    plt.show()

#######################################################################################################################
## MAIN ##
#######################################################################################################################

# y a t'il au moins un fichier en attente de traitement ?
list_files=verif_files('PROJET - BEE HAPPY/Datas/0_new_data')
#je prends le premier classeur disponible
name=list_files[0]
print("Je traite le classeur : " + name) 

# creation du format des dataframe initiaux 
meta={'ID_ruche':[],'ID_apiculteur':[], 'zipc':[], 'commune':[], 'adresse':[], 'num':[], 'latitude':[], 'longitude':[], 'x':[],'y':[]}
time_serie={'ID_ruche':[],'GDH':[], 'poids':[]}
apiculteur={'ID_apiculteur':[],'Status':[], 'zipc':[], 'commune':[], 'adresse':[], 'num':[], 'latitude':[], 'longitude':[], 'x':[],'y':[]}

# extraction des données du "classeur"
liste,liste2,liste3 = extract_datas(name)

#création des dataframes avec les données extraites   
df_ru=pd.DataFrame(liste,columns=meta)
df_ts=pd.DataFrame(liste2,columns=time_serie)
df_api=pd.DataFrame(liste3,columns=apiculteur)

#nettoyage des données
df_ru['adresse']=df_ru['adresse'].fillna('Inconnu')
df_ru['num']=df_ru['num'].fillna(0)
df_ru['adresse'] = df_ru['adresse'].str.replace("'","_")
df_ru['commune'] = df_ru['commune'].str.replace("'","_")

df_api['adresse']=df_api['adresse'].fillna('Inconnu')
df_api['num']=df_api['num'].fillna(0)
df_api['adresse'] = df_api['adresse'].str.replace("'","_")
df_api['commune'] = df_api['commune'].str.replace("'","_")

df_ts['poids']=df_ts['poids'].fillna(0)

""" ESSAYER PLUT TARD DE REMPLIR LES MANQUANTS DE DF_TS[POIDS] AVEC LA MOYENNE DES VALEURS ADJACENTES
    t_s_vides = df[ df['poids'] == '' ].index
    df.drop(indexNames , inplace=True)
"""

#Connection à la base de données
DB_host='lucky.db.elephantsql.com'
DB_name='gsjpkacq'
DB_user='gsjpkacq'
DB_pass='HrEmIikKpM_k-CFk7Va_16W0CJuZ0c82'

conn=psycopg2.connect(dbname=DB_name, user=DB_user, password=DB_pass,host=DB_host)
cur=conn.cursor()

#Inscriptions des données dans la BD 
inscription_DB(df_ru,df_ts,df_api)
conn.close()

# renomme les fichier traités et les range dans le dossier "1_exploited_files"
sauvegarde_fichiers_csv(df_ru,df_ts,df_api,name)

#supprime le fichier d'origine
# A REMETTRE APRES TESTS#######################################################################################################################
# os.remove('PROJET - BEE HAPPY/Datas/0_new_data/'+ nom_fichier)

