import os
import pandas as pd
import sqlalchemy

user = 'u719083223_root'
psw = 'Adminolist22'
host = 'sql660.main-hosting.eu'
dbname = 'u719083223_olist'
#port = 

str_connection = 'mysql+pymysql://{user}:{psw}@{host}/{dbname}'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

#Lendo os arquivos do DATA_DIR
files_names = [ i for i in os.listdir(DATA_DIR) if i.endswith('.csv') ]

#Abrindo conexão com BD
str_connection = str_connection.format( user = user, psw = psw, host = host, dbname = dbname)
connection = sqlalchemy.create_engine(str_connection)

#Escrevendo no BD as tabelas
for i in files_names:
    df_tmp = pd.read_csv( os.path.join(DATA_DIR, i))
    table_name = "tb_" + i.strip(".csv").replace("olist_", "").replace("_dataset","")
    df_tmp.to_sql( table_name, connection )

print('operação realizada!') 