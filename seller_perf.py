# -*- coding: utf-8 -*-
"""
Written and Designed by Sébastien Gahat
"""
## Pandas
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
## Dates
#import datetime
import time
from datetime import date
## Windows
import os
## Sheets API v4
import httplib2
import pandas as pd
from apiclient import discovery
import quickstart

debut = time.clock()
## étape 0 définir quel mois on veut étudier
if date.today().day<5:
    month_souhait='précédent'
else:
    month_souhait='actuel'
if month_souhait=='actuel':
    month=date.today().month
    year=date.today().year
elif date.today().month==1:
    month=12
    year=date.today().year-1
else:
    month=date.today().month-1
    year=date.today().year
    
## étape 1 import et traitement des fichiers CSV
ORDER_PATH = os.path.join(os.path.dirname(__file__), '../basedump/order.csv')
PRODUCT_PATH = os.path.join(os.path.dirname(__file__), '../basedump/product.csv')
SHOP_PATH = os.path.join(os.path.dirname(__file__), '../basedump/shop.csv')
AMID_PATH = os.path.join(os.path.dirname(__file__), 'AM_id.csv')
order_df = pd.read_csv(ORDER_PATH, dtype=str)
product_df = pd.read_csv(PRODUCT_PATH, dtype=str)
shop_df = pd.read_csv(SHOP_PATH, dtype=str)
AM_ID = pd.read_csv(AMID_PATH, encoding = "ISO-8859-1", dtype=str)
product_df=product_df[['_id','sku','createdAt','status','createdBy','shop._id','shop.name','pricing.price.value','status']]
shop_df=shop_df[['_id','slug','shopType','proType','commissionRate','displayName','user','accountManager']]
shop_df.rename(columns={'_id': 'shop_id','accountManager':'am_id'}, inplace=True)
product_df.rename(columns={'shop._id': 'shop_id'}, inplace=True)

## étape 2 faire le merge/join avec le shop manager et garder que les bons AMs
shop_df=shop_df.merge(AM_ID, on='am_id', how='left')
shop_df=shop_df[shop_df['manager'].isin(['Manon','Théo','Yann'])]

## étape 3 garder que les mis en ligne, et choper ceux du mois M et du mois M-1
a=(product_df['status'].isin(['accepted','published','awaiting_crop','removed','removed_by_seller','sold_out']))
a.columns=[0,1]
product_df['mel']=a.loc[:,0]
mis_en_ligne=product_df[product_df['mel']==True]
mis_en_ligne=mis_en_ligne.drop(['mel'], axis=1)
mis_en_ligne['année_créa']=mis_en_ligne['createdAt'].str.split('-',expand=True).loc[:,0]
mis_en_ligne['année_créa']=pd.to_numeric(mis_en_ligne['année_créa'])
mis_en_ligne['mois_créa']=mis_en_ligne['createdAt'].str.split('-',expand=True).loc[:,1]
mis_en_ligne['mois_créa']=pd.to_numeric(mis_en_ligne['mois_créa'])
mis_en_ligne_moisM=mis_en_ligne[(mis_en_ligne['année_créa']==year)&(mis_en_ligne['mois_créa']==month)]
if month==1:
    year_past=year - 1
    month_past=12
else:
    year_past=year
    month_past=month-1
mis_en_ligne_moisM_1=mis_en_ligne[(mis_en_ligne['année_créa']==year_past)&(mis_en_ligne['mois_créa']==month_past)]

## étape 4 garder les post cancel et arrangement global de l'order_df
order_df=order_df[order_df['Order status'] == 'paid']
order_df=order_df[order_df['Order product status'].isin(['TRANSFER_PROCESSED','TRANSFER_REQUESTED','SHIPPED','CONFIRMED','DELIVERED','NEW','PENDING','ABORTED'])]
order_df=order_df[['Order number','Created at','Product Sku','Seller id','GMV']]
order_df.rename(columns={'Seller id': 'user'}, inplace=True)
order_df['année_vente']=order_df['Created at'].str.split('-',expand=True).loc[:,0]
order_df['mois_vente']=order_df['Created at'].str.split('-',expand=True).loc[:,1]
order_df['année_vente']=pd.to_numeric(order_df['année_vente'])
order_df['mois_vente']=pd.to_numeric(order_df['mois_vente'])
order_df_moisM=order_df[(order_df['année_vente']==year)&(order_df['mois_vente']==month)]
order_df_moisM_etM_1=pd.concat([order_df[(order_df['année_vente']==year_past)&(order_df['mois_vente']==month_past)],order_df_moisM])

## étape 5 début du remplissage des infos
mis_en_ligne=mis_en_ligne.sort_values(by=['createdAt'], ascending=True)
shop_finaldf=pd.merge(shop_df, mis_en_ligne, on='shop_id', how='left', validate="1:m", sort=True)
shop_finaldf=shop_finaldf.sort_values(by=['createdAt'], ascending=True)
shop_finaldf=shop_finaldf.drop_duplicates(subset='shop_id')
shop_finaldf=shop_finaldf[['shop_id','slug','shopType','proType','commissionRate','displayName','user','am_id','manager','createdAt']]

## étape 6 on convertit les first prod date en datetime, et on rentre les mois/années first
shop_finaldf=shop_finaldf.reset_index(drop=True)
product_df=product_df.reset_index(drop=True)
shop_finaldf['mois_first']=pd.to_numeric(shop_finaldf['createdAt'].str.split('-',expand=True).loc[:,1])
shop_finaldf['année_first']=pd.to_numeric(shop_finaldf['createdAt'].str.split('-',expand=True).loc[:,0])

## étape 7 récupérer les nombres de produits de M et M-1
mis_en_ligne_moisM=mis_en_ligne_moisM.reset_index(drop=True)
corres_prod1=mis_en_ligne_moisM.groupby("shop_id")["sku"].count()
corres_prod1=corres_prod1.reset_index()
corres_prod1.rename(columns={'sku':'nombre de prod mis en ligne mois M'}, inplace=True)
corres_prod1.rename(columns={'index':'shop_id'})
shop_finaldf=pd.merge(shop_finaldf,corres_prod1, how='left', on='shop_id')

mis_en_ligne_moisM_1=mis_en_ligne_moisM_1.reset_index(drop=True)
corres_prod2=mis_en_ligne_moisM_1.groupby("shop_id")["sku"].count()
corres_prod2=corres_prod2.reset_index()
corres_prod2.rename(columns={'sku':'nombre de prod mis en ligne mois M-1'}, inplace=True)
corres_prod2.rename(columns={'index':'shop_id'})
shop_finaldf=pd.merge(shop_finaldf,corres_prod2, how='left', on='shop_id')

## étape 8 récupérer la GMV du mois de chaque shop
order_df_moisM=order_df_moisM.reset_index(drop=True)
order_df_moisM['GMV']=order_df_moisM['GMV'].astype(float)
corres_order=order_df_moisM.groupby("user")["GMV"].sum()
corres_order=corres_order.reset_index()
corres_order.rename(columns={'GMV':'GMV totale mois', 'index':'user'}, inplace=True)
shop_finaldf=pd.merge(shop_finaldf,corres_order, how='left', on='user')

## to be removed : temporairement rajouter la colonne vierge
shop_finaldf['nb prod sold 2 months']=''

## étape 9 récupérer la GMV de M et M-1
order_df_moisM_etM_1=order_df_moisM_etM_1.reset_index(drop=True)
order_df_moisM_etM_1['GMV']=order_df_moisM_etM_1['GMV'].astype(float)
corres_order2=order_df_moisM_etM_1.groupby("user")["GMV"].sum()
corres_order2=corres_order2.reset_index()
corres_order2.rename(columns={'GMV':'GMV last 2 months', 'index':'user'}, inplace=True)
shop_finaldf=pd.merge(shop_finaldf,corres_order2, how='left', on='user')

## étape 10 tout clean pour le rendu final
shop_finaldf['GMV totale mois']=shop_finaldf['GMV totale mois'].astype(str)
shop_finaldf['GMV last 2 months']=shop_finaldf['GMV last 2 months'].astype(str)
shop_finaldf['GMV totale mois']=shop_finaldf['GMV totale mois'].str.replace('.',',')
shop_finaldf['GMV totale mois']=shop_finaldf['GMV totale mois'].str.replace('nan','0')
shop_finaldf['GMV last 2 months']=shop_finaldf['GMV last 2 months'].str.replace('.',',')
shop_finaldf['GMV last 2 months']=shop_finaldf['GMV last 2 months'].str.replace('nan','0')
shop_finaldf['année_first']=shop_finaldf['année_first'].astype(str).str[:-2]
shop_finaldf['année_first']=shop_finaldf['année_first'].str.replace('n','0')
shop_finaldf['mois_first']=shop_finaldf['mois_first'].astype(str).str[:-2]
shop_finaldf['mois_first']=shop_finaldf['mois_first'].str.replace('n','0')
shop_finaldf['nombre de prod mis en ligne mois M']=shop_finaldf['nombre de prod mis en ligne mois M'].astype(str).str[:-2]
shop_finaldf['nombre de prod mis en ligne mois M']=shop_finaldf['nombre de prod mis en ligne mois M'].str.replace('n','0')
shop_finaldf['nombre de prod mis en ligne mois M-1']=shop_finaldf['nombre de prod mis en ligne mois M-1'].astype(str).str[:-2]
shop_finaldf['nombre de prod mis en ligne mois M-1']=shop_finaldf['nombre de prod mis en ligne mois M-1'].str.replace('n','0')
shop_finaldf['slug']=shop_finaldf['slug'].fillna(value=0)
shop_finaldf['proType']=shop_finaldf['proType'].fillna(value=0)
shop_finaldf['displayName']=shop_finaldf['displayName'].fillna(value=0)
shop_finaldf['createdAt']=shop_finaldf['createdAt'].fillna(value=0)

## étape 11 faire le travail des % products sold
corres_order3=order_df_moisM_etM_1[['Order number','Created at','Product Sku','user']]
corres_shop2=shop_df[['user','manager']]
corres_order3=pd.merge(corres_order3,corres_shop2,on='user',how='left')
corres_prod3=pd.concat([mis_en_ligne_moisM,mis_en_ligne_moisM_1])
corres_prod3=corres_prod3[['sku','createdAt']]
corres_order3.rename(columns={'Product Sku':'sku'},inplace=True)
corres_order3=pd.merge(corres_order3,corres_prod3, on='sku',how='left')
result=corres_order3.groupby('manager')['createdAt'].count()
result2=order_df_moisM.groupby('Order number').count()
print('nbre de commandes post cancel ce mois-ci : ',result2['Created at'].count())
print('manon : ', result.loc['Manon'])
print('yann : ', result.loc['Yann'])
print('theo : ', result.loc['Théo'])
prod_sold=[str(result.loc['Manon']),str(result.loc['Théo']),str(result.loc['Yann'])]
transac=str(result2['Created at'].count())

#shop_finaldf.set_index('shop_id', inplace=True)
#shop_finaldf.to_clipboard()
#print("le df a été copié dans le clipboard")

## étape 12 let's go pasting the results

## BASE ##
sent_result=shop_finaldf.as_matrix()
sent_result=sent_result.tolist()
body = {'values': sent_result}

credentials = quickstart.get_credentials()
http = credentials.authorize(httplib2.Http())
discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                'version=v4')
service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discoveryUrl)
spreadsheetId = '1zYl1DF6uBkcBxYmfYRi7Erer87pshu2lkFVhf-plYkk'

if date.today().day==5:
    requests = []
# Add one sheet to the spreadsheet each Monday (début analyse sourcing new week)
    requests.append({
            'addSheet': {
                    'properties': {
                            'sheetId':str(date.today().year)+str(date.today().month),
                            'title': date.today().strftime("%B"),
                            'index':"3"
                            }
        }
    })

    body = {
            'requests': requests
            }

    result = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId,body=body).execute()

sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheetId).execute()
sheets = sheet_metadata.get('sheets', '')
i=0
id_chosen='497065465' ## c'est l'id de la feuille correspondant à la 1ère feuille, pour Janvier
id_chosen=str(date.today().year)+str(date.today().month) ## c'est l'id de la feuille correspondant à la feuille créée
sheet_id=0
body = {'values': sent_result}
while sheet_id!=int(id_chosen):
    title = sheets[i].get("properties", {}).get("title", "Sheet1") ## get le nom de la sheet placée à l'index i
    sheet_id = sheets[i].get("properties", {}).get("sheetId", 0)
    i+=1
result = service.spreadsheets().values().update(
    spreadsheetId=spreadsheetId, range=title+'!A2:Q',
    valueInputOption='USER_ENTERED', body=body).execute()
print('{0} cells updated.'.format(result.get('updatedCells')))
print('{0} rows updated.'.format(int(result.get('updatedCells')/17)))

## % prod sold ##
id_stats='373142294' ## c'est l'id de la feuille Stats
dic = {"1":"B","2":"C","3":"D","4":"E","5":"F","6":"G","7":"H","8":"I","9":"J",
     "10":"K","11":"L","12":"M","13":"N","14":"O","15":"P","16":"Q","17":"R",
     "18":"S","19":"T","20":"U","21":"V","22":"W","23":"X","24":"Y","25":"Z",
     "26":"AA","27":"AB","28":"AC","29":"AD","30":"AE","31":"AF","32":"AG",
     "33":"AH","34":"AI","35":"AJ","36":"AK","37":"AL","38":"AM","39":"AN",
     "40":"AO","41":"AP","42":"AQ","43":"AR","44":"AS","45":"AT","46":"AU",
     "47":"AV","48":"AW","49":"AX","50":"AY","51":"AZ","52":"BA","53":"BB",
     "54":"BC","55":"BD","56":"BE","57":"BF","58":"BG","59":"BH","60":"BI",
     "61":"BJ","62":"BK","63":"BL","64":"BM","65":"BN","66":"BO","67":"BP",
     "68":"BQ","69":"BR","70":"BS","71":"BT","72":"BU","73":"BV","74":"BW",
     "75":"BX","76":"BY","77":"BZ","78":"CA","79":"CB","80":"CC","81":"CD",
     "82":"CE","83":"CF","84":"CG","85":"CH","86":"CI","87":"CJ","88":"CK",
     "89":"CL","90":"CM","91":"CN","92":"CO","93":"CP","94":"CQ","95":"CR",
     "96":"CS","97":"CT","98":"CU","99":"CV"}
check=0
liste=[]
j=28
while check==0:
    liste=[]
    j+=1
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range='Stats!'+dic[str(j)]+'13').execute()
    for k in result.keys():
        liste.append(k)
    if 'values' not in liste: check+=1

body = {'values': [prod_sold]}
if date.today().day==5:
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheetId, range='Stats!'+dic[str(j)]+'14:'+dic[str(j+2)]+'14',
        valueInputOption='USER_ENTERED', body=body).execute()
else:
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheetId, range='Stats!'+dic[str(j-3)]+'14:'+dic[str(j-1)]+'14',
        valueInputOption='USER_ENTERED', body=body).execute()

## nb transactions ##
check=0
liste=[]
j=10
while check==0:
    liste=[]
    j+=1
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range='OKR Team Sales!'+dic[str(j)]+'16').execute()
    for k in result.keys():
        liste.append(k)
    if 'values' not in liste: check+=1
    
body = {'values': [[transac]]}
result = service.spreadsheets().values().update(
    spreadsheetId=spreadsheetId, range='OKR Team Sales!'+dic[str(j-2)]+'16',
    valueInputOption='USER_ENTERED', body=body).execute()

## the end
fin=time.clock()
print("le programme a mis ",fin-debut," secondes pour tourner")