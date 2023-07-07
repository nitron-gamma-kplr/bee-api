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
import pyodbc
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.error import HTTPError
import time
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.firefox.options import Options
import shutil, os
import tqdm

# verifie la presence de fichiers dans le dossier 'new_datas". Si non, quitte le programme et si oui renvoie une liste avce le nom des fichiers présents
def verif_files(directory): 
    list_files=os.listdir(directory)
    
    nd_files = len(list_files)
    if len(list_files) ==0 :
        print("Il n'y a rien à traiter dans le dossier '0_new_data'.")
        sys.exit()
    return(list_files)

#CONNECTION A LA BD
def DB_connect():
    DB_host='lucky.db.elephantsql.com'
    DB_name='gsjpkacq'
    DB_user='gsjpkacq'
    DB_pass='HrEmIikKpM_k-CFk7Va_16W0CJuZ0c82'

    conn=psycopg2.connect(dbname=DB_name, user=DB_user, password=DB_pass,host=DB_host)
    cur=conn.cursor()
    return(conn,cur)

#extrait les données d'un classeur
def extract_datas(name):

    data_input=xlrd.open_workbook("D:/FORMATION_DATASCIENCES/PROJET_BEE-API/Datas/0_new_data/"+ name)
    nb_feuilles=data_input.nsheets
    
    liste_ru=[]
    for feuille_active in data_input:
        #metadonnes des ruches
        status=feuille_active.cell_value(0, 0)
        adresse_comp=feuille_active.cell_value(1, 0)
        nom_ruche=feuille_active.cell_value(2, 0)
        latitude, longitude, num, add, zipc, city,x,y =extract_coord(adresse_comp)
        
        #retourne l'id de la ruche
        id_ruche=get_id_ruche(nom_ruche,x,y)
        
        #inserer la un module qui va chercher l'ID de l'apiculteur correspondant à la ruche. Pour l'instant on laisse vide  
        ##############################################################################################################################
        api_id=0  
        liste_ruche=[api_id,nom_ruche,zipc,city,add,num,latitude,longitude,x,y]

        if id_ruche==-1:
            write_ruche(liste_ruche)
            id_ruche=get_id_ruche(nom_ruche,x,y)
        
        liste_ruche.insert(0, id_ruche)

        #extraction des time series
        for r in tqdm.tqdm(range(4,feuille_active.nrows),desc="Ecriture des time_séries : " + str(feuille_active.name), bar_format="{l_bar}{bar:20}{r_bar}"):
            val_courante=(feuille_active.cell_value(rowx=r, colx=0))
            GDH=xlrd.xldate_as_datetime(val_courante, 0)
            poids=feuille_active.cell_value(rowx=r, colx=1)
            if poids=="":
                poids=0
            """ ESSAYER PLUT TARD DE REMPLIR LES MANQUANTS DE DF_TS[POIDS] AVEC LA MOYENNE DES VALEURS ADJACENTES
            t_s_vides = df[ df['poids'] == '' ].index df.drop(indexNames , inplace=True) """

            liste_ts=[id_ruche,GDH,poids]
            write_ts(liste_ts)
        
        liste_ru.append(liste_ruche)  
  
    return(liste_ru , liste_ts)

#VERIFIE SI LA RUCHE EST PRESENTE EN BD
def get_id_ruche (nom_ruche,x,y):
    
    requete="SELECT * FROM ruches WHERE nom_ruche='" + nom_ruche +"' AND x='" + str(x) +"' AND y='"+ str(y) +"'"
    data= cur.execute(requete)
    result=cur.fetchall()
       
    if len(result)==0:
        id_ruche=-1
    else:
        id_ruche = result[0][0]
    return(id_ruche)

#ECRIT LA RUCHE DANS LA BASE DE DONNEES
def write_ruche(liste_ruche):
    
    #creation du dataframe
    meta=['api_id','nom_ruche', 'zipc', 'commune', 'adresse', 'num', 'latitude', 'longitude', 'x','y']
    df_ru=pd.DataFrame(columns=meta)
    df_ru.loc[len(df_ru)] = liste_ruche
    
    #nettoyage des données
    df_ru['adresse']=df_ru['adresse'].fillna('Inconnu')
    df_ru['num']=df_ru['num'].fillna(0)
    df_ru['adresse'] = df_ru['adresse'].str.replace("'","_")
    df_ru['commune'] = df_ru['commune'].str.replace("'","_")
    liste=df_ru.values.tolist()
    api_id =str(liste[0][0])
    nom_ruche =str(liste[0][1])
    zipc=str(liste[0][2])
    commune=str(liste[0][3])
    adresse=str(liste[0][4])
    num=str(liste[0][5])
    latitude=str(liste[0][6])
    longitude=str(liste[0][7])
    x=str(liste[0][8])
    y=str(liste[0][9])

    #write_ruche2(api_id , nom_ruche, zipc,commune,adresse,num,latitude,longitude,x,y)
    
def write_ruche2(api_id , nom_ruche, zipc,commune,adresse,num,latitude,longitude,x,y):
    
    api_id =api_id +","
    nom_ruche ="'"+ nom_ruche +"',"
    zipc=zipc +","
    commune="'"+ commune+"',"
    adresse="'"+ adresse +"',"
    num= num +","
    latitude=latitude +","
    longitude=longitude +","
    x=x +","
    
    param = api_id + nom_ruche + zipc + commune + adresse + num + latitude + longitude + x + y
    requete="INSERT INTO ruches (api_id , nom_ruche, zipc,commune,adresse,num,latitude,longitude,x,y) SELECT "+ param + "WHERE NOT EXISTS (SELECT api_id , nom_ruche, zipc,commune,adresse,num,latitude,longitude,x,y FROM ruches WHERE api_id ="+ api_id[0:-1] + "AND nom_ruche= "+ nom_ruche[0:-1] + "AND zipc = "+ zipc[0:-1] + "AND commune= " + commune[0:-1] + "AND  adresse= " + adresse[0:-1] + "AND num= " + num[0:-1] + "AND latitude = " + latitude[0:-1]+"AND longitude = " + longitude[0:-1] + "AND x = " + x[0:-1] + "AND y = "+ y +")"
    cur.execute (requete)
    conn.commit()

#ECRIT La time series DANS LA BASE DE DONNEES
def write_ts(liste_ts):
    #creation du dataframe
    time_serie=['id_ruche','GDH', 'poids']
    df_ts=pd.DataFrame(columns=time_serie)
    df_ts.loc[len(df_ts)] = liste_ts
    
    #nettoyage des données
    df_ts['poids']=df_ts['poids'].fillna(0)

    liste=df_ts.values.tolist()

    id_ruche=str(liste[0][0])
    GDH=str(liste[0][1])
    poids=str(liste[0][2])
    #write_ts2(id_ruche,GDH, poids)
  
def write_ts2(id_ruche,GDH, poids):
    
    id_ruche=id_ruche+","
    GDH="'"+ GDH + "',"
    param = id_ruche + GDH + poids
    
    requete="INSERT INTO time_series (id_ruche,GDH, poids) SELECT " + param + " WHERE NOT EXISTS (SELECT id_ruche,GDH, poids FROM time_series WHERE id_ruche ="+ id_ruche[0:-1] + " AND GDH = " + GDH[0:-1] +" AND poids = " + poids + ")"
    cur.execute (requete)
    conn.commit()  

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

#sauvegadre les données importées dans un fichier horodaté
def sauvegarde_fichiers(name):
    nom_fichier=name
    d = datetime.now()
    f = d.strftime('%Y%m%d%H%M%S-')
    p=('PROJET_BEE-API/Datas/1_exploited_data/')
    nom_fichier=p+f+name

    file_oldname = os.path.join("PROJET_BEE-API/Datas/0_new_data", name)
    file_newname_newfile = os.path.join(nom_fichier)
    os.rename(file_oldname, file_newname_newfile)
    
# ECRIT LES DONNEES DES PARCELLES DANS LA BASE DE DONNEES
def write_parc(df_parc):
    #Inscriptions dans la table parc
    long = len(df_parc)
    for i in tqdm.tqdm(range(long), desc="Ecriture des parcelles : ", bar_format="{l_bar}{bar:20}{r_bar}"):
        liste2=list(df_parc.iloc[i])
        id_ruche=str(liste2[0])
        id_culture=str(liste2[1])
        Bio=str(liste2[2])
        annee=str(liste2[3])
        pourcentage_surface_butinage=str(liste2[4])
        #write_parc2(id_ruche,id_culture,Bio,annee,pourcentage_surface_butinage)
        
def write_parc2(id_ruche,id_culture,Bio,annee,pourcentage_surface_butinage):
    id_ruche=id_ruche+","
    id_culture="'"+id_culture+"',"
    Bio="'"+Bio+"',"
    annee=annee+","
    
    param=id_ruche+id_culture+Bio+annee+pourcentage_surface_butinage
    requete="INSERT INTO parcelles (id_ruche,id_culture,Bio,annee,pourcentage_surface_butinage) SELECT " + param + "WHERE NOT EXISTS (SELECT id_ruche,id_culture,Bio,annee,pourcentage_surface_butinage FROM parcelles WHERE id_ruche ="+ id_ruche[0:-1] +" AND id_culture =" + id_culture[0:-1] + " AND Bio =" + Bio[0:-1] + " AND annee =" + annee[0:-1] +" AND pourcentage_surface_butinage =" + pourcentage_surface_butinage+")"
    cur.execute(requete)
    conn.commit()

# webscrapping des parcelles environant les ruches présentes dans le fichier d'entrée
def get_parcelles(emplacements):
    
    #options = Options()
    #options.add_argument("--headless")
    
    #driver = webdriver.Firefox(options=options)
    driver = webdriver.Firefox()

    website="https://appli.itsap.asso.fr/app/01-beegis"

    driver.get(website)
        
    time.sleep(15)
    '''
    #fermer le popup d'entrée
    iframe = driver.find_element(By.ID, 'shinyframe')
    driver.switch_to.frame(iframe)

    time.sleep(10)
    
    pop_up_close = driver.find_element(By.ID, "fermer")
    pop_up_close.click()
    '''
    
    #inscrire les coordonnées dans le formulaire
    for empl in emplacements:
        latitude=empl[0]
        longitude=empl[1]
        longi = driver.find_element(By.ID, "lng_cherche")
        longi.send_keys(longitude)
        lati = driver.find_element(By.ID, "lat_cherche")
        lati.send_keys(latitude)

        #valider les coordonnées courantes
        cherche_posit= driver.find_element(By.ID, "recherche_coordonnees")
        cherche_posit.click()
        time.sleep(2)
        longi.clear()
        lati.clear()

    #selectionner toutes les annees
    deroule=driver.find_element(By.XPATH, '/html/body/div/div[2]/div[1]/div/div/div[1]/div/div[2]/div/div/div[1]/div[1]/div/div/div[7]/div/div/div/button')
    deroule.click()
    time.sleep(2)
    choix_annees= driver.find_element(By.XPATH, "/html/body/div/div[2]/div[1]/div/div/div[1]/div/div[2]/div/div/div[1]/div[1]/div/div/div[7]/div/div/div/div/div[1]/div/button[1]")
    choix_annees.click()
    time.sleep(2)

    #declenche l'extraction des données
    cherche_posit= driver.find_element(By.ID, "doRPG")
    cherche_posit.click()
    time.sleep(len(emplacements*7))

    #ouvre l'onglet 'tableau'
    ouvre_tab= driver.find_element(By.XPATH, "/html/body/div/div[2]/div[1]/div/ul/li[3]/a")
    ouvre_tab.click()
    time.sleep(5)

    #télécharge le résultat
    get_result= driver.find_element(By.ID, "dlmapData")
    get_result.click()
    time.sleep(5)

    driver.quit()

    #construction du nom defichier à extraire du dossier téléchargement
    nom_fichier=datetime.today()
    nom_fi=str(nom_fichier)
    nom_fi='donnees-'+nom_fi[:10]+".csv"

    
    # Déplacer un fichier du répertoire rep1 vers rep2
    chemin_from='c:/Users/stela/Downloads/'+ nom_fi
    chemin_to='D:/FORMATION_DATASCIENCES/PROJET_BEE-API/Datas/2_data_tampon/'+ nom_fi
    shutil.move(chemin_from, chemin_to)
    df_parc=pd.read_csv("D:/FORMATION_DATASCIENCES/PROJET_BEE-API/Datas/2_data_tampon/" + nom_fi)
    return(df_parc)

def traitement_parcelles(df_parc,data_temp_ru):
    #suppression des colonnes inutilisées dans "parcelles"
    df_parc=df_parc.drop(df_parc.columns[[0,2,7,8,9]], axis=1)

    #remplacement de 'Emplacementx' par l'ID de la ruche correspondantes das le dataframe
    n=1
    for i in data_temp_ru:
        empl="Emplacement "+str(n)
        df_parc=df_parc.replace(empl,i)
        n=n+1

    #transformation de la valeur RPG en annee
    df_parc['Source']=df_parc['Source'].str[4:]


    #gestion de la colonne BIO
    df_parc['Agriculture biologique']=df_parc['Agriculture biologique'].apply(lambda x: 'Non' if x!='Oui' else 'Oui')


    #transformation en % de la surface de butinage
    surf_utile=15*15*3.14159
    df_parc["pourcentage surface butinage"]=df_parc['Surface (en ha) / Linéaire(m)']/surf_utile*100
    del df_parc['Surface (en ha) / Linéaire(m)']

    #Connection à la base de données
    conn,cur=DB_connect()

    #remplacement des ' dans le champ Culture de df_parc
    df_parc['Culture'] = df_parc['Culture'].str.replace("'", "_")


    # disable chained assignments
    pd.options.mode.chained_assignment = None

    #recherche et remplacement ID_culture
    df_cult=pd.read_sql('SELECT id_culture, nom FROM cultures', conn)
    for i in df_cult.values:
        id_cult=i[0]
        cult=i[1]
        df_parc['Culture'].loc[df_parc['Culture']==cult]=id_cult

    #suppression des valeurs rares

    df_parc = df_parc[df_parc['Culture'].str.len() <= 4]

    return(df_parc)

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
#######################################################################################################################
## MAIN ##
#######################################################################################################################
#######################################################################################################################


# y a t'il au moins un fichier en attente de traitement ?
list_files=verif_files('PROJET_BEE-API/Datas/0_new_data')

#je prends le premier classeur disponible
name=list_files[0]
print("Je traite le classeur : " + name) 

#CONNEXION A LA BASE DE DONNEES
conn,cur=DB_connect()

# extraction des données du "classeur" : metadonnées et  time series et parcelles environnantes de chaque ruche

liste_ru,liste_ts = extract_datas(name)

# recherche des cultures environant les ruches

emplacements=[]
data_temp_ru=[]
for liste_i in liste_ru:
    emplacements.append([liste_i[7],liste_i[8]])
    data_temp_ru.append(liste_i[0])
    
df_parc=get_parcelles(emplacements)
print("ok parcelles")
df_parc=traitement_parcelles(df_parc,data_temp_ru)
write_parc(df_parc)

# renomme les fichier traités et les range dans le dossier "1_exploited_files"
sauvegarde_fichiers(name)

