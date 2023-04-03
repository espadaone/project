
import pandas as pd
import pymysql 
import datetime as dt
from datetime import datetime
import numpy as np
 

    
def main():
    global df  # asegura que df esté en el ámbito global
    
    PRESCRIPTIONS= pd.DataFrame(pd.read_csv('https://raw.githubusercontent.com/espadaone/project/main/datasets/PRESCRIPTIONS.csv'))
    
    df_transfor=transform_data(PRESCRIPTIONS)  
    fill_df=fill_empty_values(df_transfor)
    prescriptions=fill_df
    print(prescriptions)
    print(prescriptions.info())

    
    load_data_to_mysql(prescriptions)
    
def transform_data(prescriptions):
    # Get the name of the DataFrame variable
    prescriptions.rename(columns ={'row_id' : 'prescriptions_id'}, inplace = True)
    # We delete the column with the patient's id
    prescriptions.drop(columns = ['subject_id'], inplace = True)
    # drop duplicates
    prescriptions.drop_duplicates(inplace = True)
    #format date
    prescriptions["startdate"] = pd.to_datetime(prescriptions["startdate"]).astype("str").replace('NaT',"1970-01-01")
    prescriptions["enddate"] = pd.to_datetime(prescriptions["enddate"]).astype("str").replace('NaT',"1970-01-01")
    df_transfor=prescriptions
    return df_transfor

def fill_empty_values(df_transfor):
    
    # fill blank spaces
    df_transfor["icustay_id"].fillna(0, inplace=True)
    df_transfor["drug_name_poe"].fillna('N/D', inplace=True)
    df_transfor["drug_name_generic"].fillna('N/D', inplace=True)
    df_transfor["formulary_drug_cd"].fillna('N/D', inplace=True)
    df_transfor["gsn"].fillna(0, inplace=True)
    df_transfor["ndc"].fillna(0, inplace=True)
    df_transfor["form_unit_disp"].fillna('N/D', inplace=True)
    df_transfor["enddate"].fillna('', inplace=True)
    df_transfor["enddate"].replace('Nat','Null')
    # We reorder the fields to match the sql tables
    fill_df = df_transfor[ ['prescriptions_id', 'hadm_id', 'icustay_id', 'startdate', 'enddate',
                                'drug_type', 'drug', 'drug_name_poe', 'drug_name_generic',
                                'formulary_drug_cd', 'gsn', 'ndc', 'prod_strength', 'dose_val_rx',
                                'dose_unit_rx', 'form_val_disp', 'form_unit_disp', 'route'] ]    
    
    return fill_df
    

# We define the function to load the data to MySQL
def load_data_to_mysql(prescriptions):
    # We connect to the database
    
    connection = pymysql.connect(
        host ='servaz.mysql.database.azure.com',
        port = 3306,
        user = 'Azadmin',
        password = 'AZcosmospf08',
        database = 'pf_uci'
    )
    
    
    # We check how is working
    print(prescriptions)
    print(prescriptions.info())
    
    # Truncate the tables so as not to lose the configuration
    cursor = connection.cursor()
    sql ="truncate table prescriptions"
    cursor.execute(sql)
    connection.commit()
    #conversion to insert to the database in a fast way
    presc = prescriptions.values.tolist()
    cursor = connection.cursor()
    cursor.executemany("""INSERT INTO prescriptions VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",presc)
    connection.commit()
    
    input(" Se cargo exitosamente.Presione Enter para salir...")


if __name__ == "__main__":
    main()
    
