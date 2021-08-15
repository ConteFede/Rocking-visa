#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importamos librerias a usar
import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy.sql.expression import true


# In[2]:


#Acceso al server
USER = 'uservs'
PASS = 'uservs123'
SERVER = 'localhost'
BBDD = 'stg_visa'


# In[3]:


#Vemos el encoding del archivo a trabajar
with open('C:/Users/fedee/Documents/Python/Final Rocking/h1b_disclosure_data_2015_2019.csv') as f:
    print(f)


# In[4]:


#Creamos motor de conexión al server
from sqlalchemy import create_engine
engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASS}@{SERVER}/{BBDD}', echo=False)


# In[5]:


#Leemos el archivo, y empezamos a analizar su composición.
def ejecutar():
    datos=pd.read_csv("C:/Users/fedee/Documents/Python/Final Rocking/h1b_disclosure_data_2015_2019.csv", encoding='cp1252', )
    print ('Filas/Columnas\n')  #Cantidad de filas y columnas
    print(datos.shape) 
    print ('\nMuestra\n')  #Imprimimos un head del contenido
    print(datos.head())
    print ('Info')  #Vemos la composición de la tabla, dtypes, y nulos
    datos.info()
    
    #Guardamos datos crudos en el server
    
    print('Cargando datos a la base...')
    try:
        engine.execute("DROP TABLE IF EXISTS tabla_test;")
        datos.to_sql("tabla_test",con=engine, if_exists = 'append', chunksize = 10000,index=False)
        print ('Carga terminada OK')
    except:
        print ('Hubo un error en la carga')
        
    #Chequeamos/cargamos errores

        column_errors.to_sql('load_errors',push_conn, 
                      if_exists = 'append', 
                      index = False, 
                      dtype={'datefld': sqlalchemy.DateTime(), 
                             'intfld':  sqlalchemy.types.INTEGER(),
                             'strfld': sqlalchemy.types.NVARCHAR(length=255),
                             'floatfld': sqlalchemy.types.Float(precision=3, asdecimal=True),
                             'booleanfld': sqlalchemy.types.Boolean})
    filas=datos.shape[0]
    return filas
    #confirmamos filas que cargamos
if __name__ == "__main__":
    print(ejecutar())


# In[6]:


#Transformamos data
def transform1c():
    print('Transformando datos...')
    try:
        engine.execute("USE stg_visa")
#Cargamos las ciudades, estado.
        engine.execute("DROP TABLE IF EXISTS stging_workplace;")
        engine.execute("""
            CREATE TABLE stging_workplace AS
                SELECT
                    CASE_NUMBER AS id,
                    WORKSITE_CITY,
                    WORKSITE_STATE_ABB,
                    WORKSITE_STATE_FULL,
                    WORKSITE
                    FROM tabla_test;""")
        print('Creación de tablas completa')
    except:
        print('Error en la creación de tablas')

if __name__ == "__main__":
    print(transform1c())


# In[7]:


#Transformaremos el worksite, que tiene varios errores de tipeo
def transform1t():
    print('modificando y corrigiendo')
    try:
        Df=pd.read_sql('SELECT * FROM stging_workplace', engine) #conectamos a la nueva tabla
        print('conectado a la tabla')
    except:
        print('no se pudo conectar')
    try:
        print(Df.head())   #vemos un head de la nueva tabla
        print('Duplicados total')
        print(Df.duplicated().sum() ) #vemos la cantidad de duplicados
        print('Duplicados case id')
        print(Df['id'].duplicated().sum()) #duplicados especificos de worksite
        print('Duplicados worksite')
        print(Df['WORKSITE'].duplicated().sum()) #duplicados especificos de worksite
        print('conta nulos') 
        print('nulos')
        print(Df.isnull().sum()) #contamos nulos
        print(Df['WORKSITE'].value_counts()) #vemos el contenido de worksite
    except:
        print('error de esta parte')
        
    def a_mayusc(x): #Habiendo visto que varios worksite estan en minuscula, pasamos todos a mayusc. a fin de normalizar
        if type(x)==str:
            return x.upper()
        else:
            return x
    try:
        print('reemplazando valores')
        Dfw1 = Df.applymap(a_mayusc) #aplicamos applymap con la función mayusc definida
        Dfw1['WORKSITE'] = Dfw1['WORKSITE'].str.replace(',,',',') #reemplazamos worksite que esten con dos comas por una
        
        print(Dfw1.head())
        print(Dfw1['WORKSITE'].value_counts()) #vemos el contenido corregido del worksite
    except:
        print('fallo reemplazar valores')

    try:
        WORKSITES = Dfw1["WORKSITE"].str.rsplit(",", n = 1, expand = True) #dividimos estado de ciudad
        Dfw1["Rest_Name"]= WORKSITES[0] #definimos el nombre de la primera columna
        Dfw1['State_Name']= WORKSITES[1] #defnimos el de la segunda
        print('repartiendo')
        print(Dfw1.head()) #vemos el head nuevo
        print(Dfw1['State_Name'].value_counts()) #contenido de state
        print(Dfw1['Rest_Name'].value_counts()) #contenido de rest
        Dfw1['Rest_Name'].value_counts(ascending=True,normalize=True).tail(20).plot.barh()
        plt.show()
        Dfw1['State_Name'].value_counts(ascending=True,normalize=True).tail(20).plot.barh()
        plt.show()  
    except:
        print ('fallo div')
    try:
        engine.execute("DROP TABLE IF EXISTS idt_wksc;")
        Dfw1.info()
        Dfw1.drop_duplicates(Dfw1.columns[[0]], inplace = True)
        print('eliminados dup 0')
        Dfw1.info()
        Dfw1.to_sql("idt_wksc",con=engine, if_exists = 'append', chunksize = 10000,index=False)
        print ('Carga terminada Dfw1 -OK')
    except:
        print ('Hubo un error en la carga')
        
    try:
        Dfw1.info()
        Dfw1.drop_duplicates(Dfw1.columns[[5]], inplace = True)
        print('eliminados dup 5')
        Dfw1.info()
    except:
        print('error en eliminar duplicados')
        
    try:
        engine.execute("DROP TABLE IF EXISTS t_wksc;")
        Dfw1.to_sql("t_wksc",con=engine, if_exists = 'append', chunksize = 10000,index=False)
        print ('Carga terminada Dfw1mod -OK')
    except:
        print ('Hubo un error en la carga')
        


if __name__ == "__main__":
        transform1t()
        
        


# In[8]:


#Buscamos un db de ciudades de US.
datosus=pd.read_csv("C:/Users/fedee/Downloads/US/US.txt", sep='\t',encoding='cp1252', index_col=None)

datosus.info()

#datosus.columns=['Country_code','Postal_code', 'Place_name', 'State_name', 'State_code', 'Country_name', 'Country_code', 'Comunity_name', 'Comunity_code', 'Lat', 'Long']

datosus.drop(datosus.columns[[0, 1, 6, 7, 8, 9, 10, 11]], axis=1, inplace= True)

datosus.info()

datosus.columns=['Place_name', 'State_name', 'State_code', 'Country_name']

datosus['Place_name'] = datosus['Place_name'].astype('str')

datosus.reset_index()

#datosus[0].astype(basestring)

datosus.info()
    
try:
    datosus.drop_duplicates(datosus.columns[[0]], inplace = True)
except:
    print('error en eliminar duplicados')
    
def a_mayusc(x): #Habiendo visto que varios us cities estan en minuscula, pasamos todos a mayusc. a fin de normalizar
        if type(x)==str:
            return x.upper()
        else:
            return x

Dfdatosus = datosus.applymap(a_mayusc)


try:
    engine.execute("DROP TABLE IF EXISTS tabla_US;")
    Dfdatosus.to_sql("tabla_US",con=engine, if_exists = 'append', chunksize = 10000,index=False)
    print ('Carga terminada OK')
except:
    print ('Hubo un error en la carga')


# In[8]:


#viendo que hay varios lugares analizamos la comparación de ciudades vs. un database de ciudades de eeuu.
from difflib import SequenceMatcher

dfbase = pd.read_sql('SELECT Rest_Name FROM t_wksc', engine) #conectamos a la nueva tabla trabajada de ciudades
dfref = pd.read_sql('SELECT Place_name FROM tabla_US', engine) #conectamos a la tabla de ref

print(dfbase.value_counts())
print(dfref.value_counts())

data_dict = {"Rest_Name":[],"Place_name":[],"ratio":[]}
for x in dfbase:
    for y in dfref:
        ratio = SequenceMatcher(None, y, x).ratio() 
        data_dict["Rest_Name"].append(x)
        data_dict["Place_name"].append(y)
        data_dict["ratio"].append(ratio)
        
df_ratio = pd.DataFrame(data_dict)
print(df_ratio)


# In[9]:


#Empresa/Rubro/Posicion/Jornada
#traemos datos de la tabla parra trabajarlos
Dferpj=pd.read_sql('SELECT * FROM tabla_test', engine)
#creamos valor llave para identificar unicos
Dferpj['Kerpj'] = Dferpj['EMPLOYER_NAME'].astype(str)+'_'+Dferpj['SOC_NAME'].astype(str)+'_'+Dferpj['JOB_TITLE'].astype(str)+'_'+Dferpj['FULL_TIME_POSITION'].astype(str)
print(Dferpj.head())
Dferpj.info()

#eliminamos duplicados, dejamos valores unicos (combinaciones)
df_erjp = Dferpj.drop_duplicates(subset = "Kerpj")
df_erjp.info()
#creamos una columna que sea llave entre ambas bases
df_erjp['idk']=df_erjp.index
print(df_erjp.head())
df_erjp['idk'] = df_erjp['idk'].astype('str')


#vinculamos el valor llave y traemos la llave a la base principal
Dferpj.insert(2, 'idk', Dferpj['Kerpj'].map(df_erjp.set_index('Kerpj')['idk']))
print(Dferpj.head())

#guardamos las tablas nuevas trabajadas
try:
    engine.execute("DROP TABLE IF EXISTS stging_soc;")
    df_erjp.to_sql("stging_soc",con=engine, if_exists = 'append', chunksize = 10000,index=False)
    print ('Carga 1 terminada OK')
except:
    print ('Hubo un error en la carga')
try:
    engine.execute("DROP TABLE IF EXISTS stging_tabla_test;")
    Dferpj.to_sql("stging_tabla_test",con=engine, if_exists = 'append', chunksize = 10000,index=False)
    print ('Carga 2 terminada OK')
except:
    print ('Hubo un error en la carga')


# In[30]:


#Analisis sueldo y base
import seaborn as sns
print('cargando')
try:
    print('conectando a la base')
    Dfpw=pd.read_sql('SELECT CASE_NUMBER, PREVAILING_WAGE, YEAR, idk, CASE_STATUS FROM stging_tabla_test', engine)
    print('conectado a la base')
except:
    print('la base no se conecto')
    
#duplicados de case
print(Dfpw['CASE_NUMBER'].duplicated().sum())
Dfpw.info()
try:
    Dfpw.drop_duplicates(Dfpw.columns[[0]], inplace = True)
except:
    print('error en eliminar duplicados')
Dfpw.info()
    
print(Dfpw['YEAR'].value_counts())
print(Dfpw['PREVAILING_WAGE'].value_counts())

#Dfpw['PREVAILING_WAGE'].describe()
#sns.distplot(Dfpw['PREVAILING_WAGE'])

#sns.jointplot(x='PREVAILING_WAGE', y='YEAR', data=Dfpw)

#variación en años
var = 'YEAR'
data = pd.concat([Dfpw['PREVAILING_WAGE'], Dfpw[var]], axis=1)
data.plot.scatter(x=var, y='PREVAILING_WAGE', alpha = 0.5);



#variación en estado
var = 'CASE_STATUS'
data = pd.concat([Dfpw['PREVAILING_WAGE'], Dfpw[var]], axis=1)
data.plot.scatter(x=var, y='PREVAILING_WAGE', alpha = 0.5);

try:
    engine.execute("DROP TABLE IF EXISTS stg_baseid;")
    Dfpw.to_sql("stg_baseid",con=engine, if_exists = 'append', chunksize = 10000,index=False)
    print ('Carga terminada OK')
except:
    print ('Hubo un error en la carga')

#from seaborn import lmplot

#lmplot('PREVAILING_WAGE', 'WORKSITE', data=Dfpw)

#from seaborn import boxplot
#boxplot(Dfpw.PREVAILING_WAGE)


# In[10]:


#Creamos esquema del DW
BBDD2 = 'dw_visa'

engine2 = create_engine(f'mysql+mysqlconnector://{USER}:{PASS}@{SERVER}/{BBDD2}', echo=False)


if __name__ == '__main__':
    engine2.execute("USE dw_visa;")
    engine2.execute("SET FOREIGN_KEY_CHECKS = 0;")

    ## Rel - id - worksite
    engine2.execute("DROP TABLE IF EXISTS dw_rel1;")
    engine2.execute("""CREATE TABLE dw_rel1(c_id VARCHAR(50), Rest_Name VARCHAR(50),
    PRIMARY KEY(c_id));""")
    ## Dimension WORKSITE
    engine2.execute("DROP TABLE IF EXISTS dw_worksite;")
    engine2.execute("""CREATE TABLE dw_worksite(Rest_Name VARCHAR(50),
    State_Name VARCHAR(50),
    WORKSITE_STATE_ABB VARCHAR(50),
    WORKSITE VARCHAR(50),
    PRIMARY KEY(Rest_Name));""")
    ## Dimension SOCIEDAD/Empleo
    engine2.execute("DROP TABLE IF EXISTS dw_soc;")
    engine2.execute("""CREATE TABLE dw_soc(soc_code VARCHAR(50),
    soc_name VARCHAR(255),
    soc_rub VARCHAR(255),
    puesto VARCHAR(255),
    jornada VARCHAR(50),
    PRIMARY KEY(soc_code));""")
    ##Proced union
    engine2.execute("DROP TABLE IF EXISTS dw_proc;")
    engine2.execute("""CREATE TABLE dw_proc(case_id VARCHAR(50),
    estado VARCHAR(50),
    year VARCHAR(50),
    empresa_code VARCHAR(50),
    sueldo INT,
    PRIMARY KEY(case_id),
    FOREIGN KEY(case_id) REFERENCES dw_rel1(c_id),
    FOREIGN KEY(empresa_code) REFERENCES dw_soc(soc_code));""")
    
    
    engine2.execute("SET FOREIGN_KEY_CHECKS = 1;")
    


# In[11]:


#CARGAMOS DW 1
if __name__ == '__main__':
    engine2.execute("USE dw_visa;")
    engine2.execute("""INSERT INTO dw_worksite(Rest_Name, State_Name, WORKSITE_STATE_ABB, WORKSITE) SELECT Rest_Name, State_Name, WORKSITE_STATE_ABB, WORKSITE FROM stg_visa.t_wksc;""")
    engine2.execute("""INSERT INTO dw_rel1(c_id, Rest_Name) SELECT id, Rest_Name FROM stg_visa.idt_wksc;""")


# In[12]:


#CARGAMOS DW 2
if __name__ == '__main__':
    engine2.execute("""INSERT INTO dw_soc(soc_code, soc_name, soc_rub, puesto, jornada) SELECT idk, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION FROM stg_visa.stging_soc;""")


# In[13]:


#CARGAMOS DW 3
if __name__ == '__main__':
    engine2.execute("""INSERT INTO dw_proc(case_id, estado, year, empresa_code, sueldo) SELECT CASE_NUMBER, CASE_STATUS, YEAR, idk, PREVAILING_WAGE FROM stg_visa.stg_baseid;""")


# In[ ]:




