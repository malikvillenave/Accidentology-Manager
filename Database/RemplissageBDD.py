import psycopg2
import pandas as pd
import numpy as np
import math
import glob
import os

def grav(x):
    return {
        1: "Indemne",
        2: "Tué",
        3: "Blessé hospitalisé",
        4: "Blessé léger"
    }[x]

connectionString = "dbname=accidentology user=postgres host=localhost password=postgres port=5432"

try:
    conn = psycopg2.connect(connectionString)
    cursor = conn.cursor()
except:
    print("Connection failed")

carac = pd.DataFrame(pd.read_csv("merge_data/caracteristiques.csv"))
carac = carac.drop(["Unnamed: 0","lat","long"],axis=1)

lieux = pd.DataFrame(pd.read_csv("merge_data/lieux.csv"))
lieux = lieux.drop(["Unnamed: 0"],axis=1)

usager = pd.DataFrame(pd.read_csv("merge_data/usager.csv"))
usager = usager.drop(["Unnamed: 0"],axis=1)

veh = pd.DataFrame(pd.read_csv("merge_data/vehicules.csv"))
veh = veh.drop(["Unnamed: 0"],axis=1)

#acc = pd.DataFrame(pd.read_csv("indicateurs_niort220km.csv"))

path = r'indicateurs'
all_files = glob.glob(os.path.join(path, "*.csv"))

df_from_each_file = (pd.read_csv(f) for f in all_files)
acc = pd.concat(df_from_each_file, ignore_index=True)

acc = acc.drop(["Unnamed: 0"],axis=1)
acc['lat'].replace('', np.nan, inplace=True)
acc['long'].replace('', np.nan, inplace=True)
acc.dropna(subset=['lat','long'], inplace=True)

merged = acc.merge(carac, on=['Num_Acc'])
merged = merged.merge(lieux, on=['Num_Acc'])
merged = merged.merge(usager, on=['Num_Acc'])
merged = merged.merge(veh, on=['Num_Acc','num_veh'])

merged.to_csv("output.csv", index=False)

for row in merged.itertuples(index=True, name='Pandas'):
    heuremin = getattr(row,"hrmn")
    heures = heuremin/100 #int de 0 à 23
    minutes = heuremin%100 #int de 0 à 59
    genrenum = getattr(row,"sexe")
    genreletter = 'U'
    if genrenum == 1:
        genreletter = 'H'
    else:
        genreletter = 'F'
    age = 2000 + getattr(row,"an") - getattr(row,"an_nais")
    atm = getattr(row,"atm")
    if not math.isnan(age) and not math.isnan(atm):
        print(str(int(getattr(row,"Num_Acc"))))

        rqt = ("INSERT INTO usager_accidente_par_vehicule "
            "VALUES ('" + str(getattr(row,"num_veh")) + "', "                                                   #Num_vehicule
            "(SELECT id_date FROM public.\"Date\" WHERE annee="+str(int(2000+getattr(row,"an")))+" AND jour="+str(getattr(row,"jour"))+" AND mois="+str(getattr(row,"mois"))+"), "                  #id date
            "" + str(int(getattr(row,"Num_Acc"))) + ", "                                                             #Num accident
            "(SELECT id_heure FROM public.\"Heure\" WHERE heure="+str(int(heures))+" AND minute="+str(minutes)+"), " #id heure
            "" + str(getattr(row,"catr")) + ", "                                                                #id type route
            "" + str(getattr(row,"catv")) + ", "                                                                #id type vehicule
            "(SELECT id_usager FROM public.usager WHERE genre='"+str(genreletter)+"' AND age="+str(age)+" AND num_usager="+str(getattr(row,"catu"))+"), "               #id usager
            "" + str(atm) + ", "                                                                                #id meteo
            "" + str(getattr(row,"lat")/100000) + ", "                                                                 #latitude
            "" + str(getattr(row,"long")/100000) + ", "                                                                #longitude
            "'" + grav(getattr(row,"grav")) + "', "                                                             #gravite
            "" + str(getattr(row,"ind")) + " "                                                                  #indicateur
            ") ON CONFLICT DO NOTHING")
        cursor.execute(rqt)
        conn.commit()
