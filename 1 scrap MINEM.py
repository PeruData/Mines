from bs4            import BeautifulSoup
from selenium       import webdriver
from urllib.parse   import quote
from urllib.request import urlopen
from urllib.request import urlretrieve
import numpy as np
import pandas as pd
import os
import re
import shutil
import time
os.chdir('/Users/Sebastian/Documents/Papers/Mines SSB/00_Data/Mines/Production/')
minerals=["CADMIO","COBRE","ESTAÑO","HIERRO","MOLIBDENO","ORO","PLATA","PLOMO","TUNGSTENO","ZINC"]
def to_ascii(x): #handles common non-ASCII characters
    if type(x) == str:
        original    = ['á','é','í','ó','ú','Á','É','Í','Ó','Ú','°','º','ñ','Ñ','ü','ÿ','Ÿ','¿','“','”','–','"','’','‘','‰']
        modified    = ['a','e','i','o','u','A','E','I','O','U','','','nh','NH','u','y','Y','' ,'' ,'' ,'' ,'' ,'' ,'' ,'%']
        for i in range(len(original)):
            x         = x.replace(original[i],modified[i])
        x = x.upper()
        x = x.replace("REFINACION", "REFINERIA")
    return x

#1) Scrap .xlsx files
reset_raw = False
if reset_raw == True:
    try:
        shutil.rmtree('intermediate/csv')
    except:
        print('file has not been created yet')
    os.mkdir('intermediate/csv')

def get_spreadsheets(url, year_start, year_end,file_type):
    driver = webdriver.Firefox()  #requires geckodriver, modify if scrapping with chromedriver
    year_post = year_end + 1
    driver.get(url)
    temp_path = 'in/Raw/temp.html'
    with open(temp_path, 'w') as f:
        f.write(driver.page_source)
    MINEM_bs = BeautifulSoup(open(temp_path), 'html.parser')
    os.remove(temp_path)

    for min in minerals:
        for yy in range(year_start, year_post):
            print('scrapping production of {0} during {1}'.format(min, yy))
            time.sleep(0.1)
            table = MINEM_bs.find('strong', text = re.compile('{0}'.format(yy))).find_parent('table')
            try:
                if min == 'ESTAÑO' and yy == 2017:
                    min_url = min.split('Ñ')[0] + quote('Ñ') + min.split('Ñ')[1]
                else:
                    min_url = min
                url_suffix = table.find('a', href = re.compile('{0}'.format(min_url))).attrs['href']
                url = 'http://www.minem.gob.pe' + url_suffix
                if min == 'ESTAÑO' and yy != 2017:
                    url = url.split('Ñ')[0] + quote('Ñ') + url.split('Ñ')[1]
                urlretrieve(url, 'in/Raw/{0}_{1}.{2}'.format(min, yy, file_type))
            except:
                print('There is no data about production of {0} during the year {1}'.format(min,yy))
    driver.close()

url_01_10 = 'http://www.minem.gob.pe/_estadistica.php?idSector=1&idEstadistica=6908'
url_11_17 = 'http://www.minem.gob.pe/_estadistica.php?idSector=1&idEstadistica=12501'

start_time = time.time()
get_spreadsheets(url_01_10,2001,2010, 'xls')
get_spreadsheets(url_11_17,2011,2017, 'xlsx')
print('TOOK {0}s'.format(time.time()-start_time))
#takes around 5 min (but selenium may crash before retrieving every spreadsheet)

#2) Merge into a .dta file
#MASTER:
var_names = ['titular','unidad', 'year', 'mineral', 'departamento', 'provincia', 'distrito', 'prod_quant', 'concentracion', 'fundicion', 'refineria']
df_master = pd.DataFrame(columns = var_names)

pd.options.mode.chained_assignment = None  #turn off 'nested assignment' pandas warning
for yy in range(2001,2018):
    for min in minerals:
        if yy < 2011:
            xls_path = 'in/Raw/{0}_{1}.xls'.format(min, yy)
        else:
            xls_path = 'in/Raw/{0}_{1}.xlsx'.format(min, yy)
        try:
            df_temp = pd.read_excel(xls_path)
            df_temp = df_temp.applymap(to_ascii)
            #print(df_temp.head())
        except:
            print('There is no data about production of {0} during {1}'.format(min, yy))
            continue
        varnames_pos = df_temp[df_temp['Unnamed: 2'].isnull() == False].first_valid_index()
        for i, var in enumerate(df_temp.columns):
            if i == 0:
                continue
            df_temp = df_temp.rename(columns = {var: df_temp.iloc[varnames_pos,i]})
        col_names = df_temp.columns
        keywords = ['concentracion', 'fundicion', 'refineria']
        for key in keywords:
            df_temp[key.lower()] = np.nan

        if (yy < 2006) and not (yy == 2005 and min == 'ORO'):
            df_temp[col_names[0]] = df_temp[col_names[0]].replace(np.nan, '', regex=True) #remove nan on firt var so that we can check conditionals against it
            #get concentracion/fundicion/refineria dummies
            for i, key in enumerate(keywords):
                j = i - 1
                k = i - 2
                df_temp.loc[df_temp[col_names[0]].str.contains(key), key] = 1
                df_temp[key] = df_temp[key].ffill()
                df_temp[key] = df_temp[key].replace(np.nan, 0, regex=True)
                if  i>0:
                    df_temp.loc[df_temp[key] == 1, keywords[j]] = 0
                if  i>1:
                    df_temp.loc[df_temp[key] == 1, keywords[k]] = 0
            vars_tokeep = [1,2,3,4,5,-4,-3,-2,-1]
            df_temp = df_temp.iloc[:,vars_tokeep]
            df_temp = df_temp.rename(columns = {df_temp.columns[0]: 'TITULAR'}) #this is necessary because else "nan" varname on some cases can cause conflicts

        else:
            for key in keywords:
                df_temp[key] = 0
                df_temp.loc[df_temp['ETAPA'] == key.upper(), key] = 1

            vars_tokeep = [4, 5, 6, 7 ,8, -4, -3, -2, -1]
            df_temp = df_temp.iloc[:,vars_tokeep]
            df_temp.head()

        df_temp['TITULAR'] = df_temp['TITULAR'].ffill()
        df_temp = df_temp.dropna(subset=[df_temp.columns[1]])
        df_temp = df_temp[1:]
        renames_dictionary = {'EMPRESA MINERA':'titular', 'UNIDAD MINERA':'unidad',
                                      'REGION': 'departamento', 'TOTAL': 'prod_quant',
                                      'T O T A L': 'prod_quant', 'TOTAL GENERAL': 'prod_quant',
                                      'ENE-DIC': 'prod_quant'}
        df_temp = df_temp.rename(columns = renames_dictionary)
        for var in ['TITULAR', 'UNIDAD', 'DEPARTAMENTO', 'PROVINCIA', 'DISTRITO']:
            df_temp = df_temp.rename(columns = {'{0}'.format(var): '{0}'.format(var.lower())})
        df_temp['year'] = yy
        df_temp['mineral'] = to_ascii(min)
        df_master = df_master.append(df_temp, sort = True)
    print("done with year {0}".format(yy))
    print(" ")

#Clean 'df_master' to enable 'to_stata' method
str_vars = ['titular', 'unidad', 'departamento', 'provincia', 'distrito', 'mineral']
for var in str_vars:
    df_master[var] = df_master[var].astype(str)
df_master['year'] = df_master['year'].astype(int)
df_master['prod_quant'] = df_master['prod_quant'].astype(float)
df_master.head()

df_master = df_master.iloc[:,[10, 4, 5, 8, 9, 1, 6, 2, 0, 3, 7]]
df_master.head()

df_master.to_stata('in/production_raw.dta')
