
import pandas as pd
import pymysql 
import time
import datetime
from datetime import datetime
 

diagnoses_icd = pd.read_csv('https://raw.githubusercontent.com/espadaone/project/main/datasets/DIAGNOSES_ICD.csv',dtype = 'str')
drgcodes = pd.read_csv('https://raw.githubusercontent.com/espadaone/project/main/datasets/DRGCODES.csv')
icustays = pd.read_csv('https://raw.githubusercontent.com/espadaone/project/main/datasets/ICUSTAYS.csv')
#inputevents_mv = pd.read_csv('https://raw.githubusercontent.com/espadaone/project/main/datasets/INPUTEVENTS_CV.csv')
prescriptions = pd.read_csv('https://raw.githubusercontent.com/espadaone/project/main/datasets/PRESCRIPTIONS.csv')

    


def main():
    
    transformacion_datos()
    connection = pymysql.connect(
        host = 'servaz.mysql.database.azure.com',
        user = 'Azadmin',
        password = 'AZcosmospf08',
        db = 'pf_uci'
    )
    carga_datos()
    #query to consult data
    
    cursor = connection.cursor()
    

def transformacion_datos():
    diagnoses_icd = pd.read_csv('https://raw.githubusercontent.com/espadaone/project/main/datasets/DIAGNOSES_ICD.csv',dtype = 'str')
    drgcodes = pd.read_csv('https://raw.githubusercontent.com/espadaone/project/main/datasets/DRGCODES.csv')
    icustays = pd.read_csv('https://raw.githubusercontent.com/espadaone/project/main/datasets/ICUSTAYS.csv')
    #inputevents_mv = pd.read_csv('https://raw.githubusercontent.com/espadaone/project/main/datasets/INPUTEVENTS_CV.csv')
    prescriptions = pd.read_csv('https://raw.githubusercontent.com/espadaone/project/main/datasets/PRESCRIPTIONS.csv')
   
    # DRGCODES
    # Renombramos la columna que sera la clave primaria
    drgcodes.rename(columns ={'row_id' : 'drgcodes_id'}, inplace = True)
    # Eliminamos la columna con el id del paciente
    drgcodes.drop(columns = ['subject_id'], inplace = True)
    # Eliminamos duplicados
    drgcodes.drop_duplicates(inplace = True)
    #datos rellenados
    drgcodes["drg_severity"].fillna(0, inplace=True)
    drgcodes["drg_mortality"].fillna(0, inplace=True)
    # Reordena las columnas para que coincidan con las tablas de sql
    drgcodes = drgcodes[ ['drgcodes_id', 'hadm_id', 'drg_type', 'drg_code',
                        'description', 'drg_severity', 'drg_mortality'] ]
    
    # Hay un valor en el campo icustay faltante, como es solo uno lo busque manualmente y en la base de datos
    # Puede especificarce que no admita nulos en SQL para que no vuelva a pasar este problema.
    #outputevents[outputevents["icustay_id"].isna()] 
    #icustays[icustays["hadm_id"] == 163189] # Buscamos el valor que nos dio en la visualizacion de arriba
    #outputevents.loc[7939,"icustay_id"] = 239396 # aqui es donde ago la modificacion, (Pueden borrase las lineas de arriba ya que esta es la que hace el cambio)
    #outputevents["icustay_id"] = outputevents["icustay_id"].astype("int64") # Se hace el cambio de tipo de dato a int64 para que coincida con las demas tablas
    # Reordenamos los campos para que coincidan con las tablas de sql
    outputevents = outputevents[ ['outputevents_id', 'hadm_id', 'icustay_id', 'charttime', 'itemid', 'value',
                                'valueuom', 'storetime', 'cgid', 'stopped', 'newbottle', 'iserror'] ]
    # PRESCRIPTIONS
    # Renombramos la columna que sera la clave primaria
    prescriptions.rename(columns ={'row_id' : 'prescriptions_id'}, inplace = True)
    # Eliminamos la columna con el id del paciente
    prescriptions.drop(columns = ['subject_id'], inplace = True)
    # Eliminamos duplicados
    prescriptions.drop_duplicates(inplace = True)
    prescriptions["startdate"] = pd.to_datetime(prescriptions["startdate"]).astype("str").replace('NaT',"1970-01-01")
    prescriptions["enddate"] = pd.to_datetime(prescriptions["enddate"]).astype("str").replace('NaT',"1970-01-01")
    # Rellenamos valores faltantes
    prescriptions["icustay_id"].fillna(0, inplace=True)
    prescriptions["drug_name_poe"].fillna('N/D', inplace=True)
    prescriptions["drug_name_generic"].fillna('N/D', inplace=True)
    prescriptions["formulary_drug_cd"].fillna('N/D', inplace=True)
    prescriptions["gsn"].fillna(0, inplace=True)
    prescriptions["ndc"].fillna(0, inplace=True)
    prescriptions["form_unit_disp"].fillna('N/D', inplace=True)
    prescriptions["enddate"].fillna('', inplace=True)
    prescriptions["enddate"].replace('Nat','Null')
    # Reordenamos los campos para que coincidan con las tablas de sql
    prescriptions = prescriptions[ ['prescriptions_id', 'hadm_id', 'icustay_id', 'startdate', 'enddate',
                                    'drug_type', 'drug', 'drug_name_poe', 'drug_name_generic',
                                    'formulary_drug_cd', 'gsn', 'ndc', 'prod_strength', 'dose_val_rx',
                                    'dose_unit_rx', 'form_val_disp', 'form_unit_disp', 'route'] ]
    
    

def carga_datos():
    # ___________________________________________________________
    # CARGA A SQL
    connection = pymysql.connect(
        host = 'servaz.mysql.database.azure.com',
        user = 'Azadmin',
        password = 'AZcosmospf08',
        db = 'pf_uci'
    )
    cursor = connection.cursor()
    # Creación de la tabla auditoría
    data_auditoria = {'fecha_creacion': [],'name_table': [], 'quantity_rows_original': [],'quantity_rows_sql': [], 'time_load': [], 'estado': []}
    auditoria = pd.DataFrame(data_auditoria)
    
    # PRESCRIPTIONS
    start_time = time.time()
    # Tuncar las tablas para no perder la configuración
    cursor = connection.cursor()
    sql ="truncate table prescriptions"
    cursor.execute(sql)
    connection.commit()
    
    presc = prescriptions.values.tolist()
    cursor = connection.cursor()
    cursor.executemany("""INSERT INTO prescriptions VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",presc)
    connection.commit()
    end_time = time.time()
    # name_table
    name_table='prescriptions'                                # CAMBIAR NOMBRE DE TABLA
    #quantity_rows_original
    quantity_rows_original=prescriptions.shape[0]              # CAMBIAR NOMBRE DE TABLA
    #___________________________________________________________
    # time
    fecha_creacion = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    fecha_creacion = str(fecha_creacion)
    #quantity_rows_sql
    cursor = connection.cursor()
    sql ="SELECT COUNT(*) FROM "+name_table+""""""
    cursor.execute(sql)
    result = cursor.fetchone()
    quantity_rows_sql=result[0]
    connection.commit()
    #time_load
    time_load= round(int(end_time - start_time))
    # Estado
    if quantity_rows_original==quantity_rows_sql:
        estado='Carga Normal'
    else:
        estado='Carga incompleta'
    # Ingesta de resultados de auditoria
    new_audit = {'fecha_creacion': fecha_creacion, 'name_table': name_table, 'quantity_rows_original': quantity_rows_original, 'quantity_rows_sql': quantity_rows_sql, 'time_load': time_load, 'estado': estado }
    # add the new row to the DataFrame
    auditoria = auditoria.append(new_audit, ignore_index=True)
    
    # DRGCODES
    start_time = time.time()
    # Tuncar las tablas para no perder la configuración
    cursor = connection.cursor()
    sql ="truncate table drgcodes"
    cursor.execute(sql)
    connection.commit()
    # Crea una lista de los valores de la Tabla
    chart = drgcodes.values.tolist()
    # Insertar valores
    cursor = connection.cursor()
    cursor.executemany("""INSERT INTO drgcodes VALUES (%s,%s,%s,%s,%s,%s,%s)""",chart)
    connection.commit()
    end_time = time.time()
    # name_table
    name_table='drgcodes'                                # CAMBIAR NOMBRE DE TABLA
    #quantity_rows_original
    quantity_rows_original=drgcodes.shape[0]              # CAMBIAR NOMBRE DE TABLA
    #___________________________________________________________
    # time
    fecha_creacion = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    fecha_creacion = str(fecha_creacion)
    #quantity_rows_sql
    cursor = connection.cursor()
    sql ="SELECT COUNT(*) FROM "+name_table+""""""
    cursor.execute(sql)
    result = cursor.fetchone()
    quantity_rows_sql=result[0]
    connection.commit()
    #time_load
    time_load= round(int(end_time - start_time))
    # Estado
    if quantity_rows_original==quantity_rows_sql:
        estado='Carga Normal'
    else:
        estado='Carga incompleta'
    # Ingesta de resultados de auditoria
    new_audit = {'fecha_creacion': fecha_creacion, 'name_table': name_table, 'quantity_rows_original': quantity_rows_original, 'quantity_rows_sql': quantity_rows_sql, 'time_load': time_load, 'estado': estado }
    # add the new row to the DataFrame
    auditoria = auditoria.append(new_audit, ignore_index=True)

    # CARGA DE AUDITORIAS
    # Cambio de tipo de dato
    #auditoria['fecha_creacion'] = pd.to_datetime(auditoria['fecha_creacion'])
    auditoria['fecha_str'] = auditoria['fecha_creacion'].astype("str")
    auditoria['fecha_str'] = auditoria['fecha_str'].str.replace('-', '').str.replace(' ', '').str.replace(':', '')
    # Concatenar la cadena de fecha y el número de fila
    auditoria['id_auditoria'] =auditoria['fecha_str'] + auditoria.index.astype(str)
    # Eliminar la columna 'fecha_str' si no es necesaria
    auditoria.drop('fecha_str', axis=1, inplace=True)
    # Cambio de Tipo de variables
    auditoria["name_table"] = auditoria["name_table"].astype("str")
    auditoria["quantity_rows_original"] = auditoria["quantity_rows_original"].astype("int")
    auditoria["quantity_rows_sql"] = auditoria["quantity_rows_sql"].astype("int")
    auditoria["time_load"] = auditoria["time_load"].astype("int")
    auditoria["estado"] = auditoria["estado"].astype("str")
    auditoria['fecha_creacion'] = pd.to_datetime(auditoria['fecha_creacion'])
    auditoria["fecha_creacion"] = auditoria["fecha_creacion"].astype("str")
    auditoria["id_auditoria"] = auditoria["id_auditoria"].astype("str")
    #Reordenar columnas
    auditoria = auditoria[ ['id_auditoria', 'fecha_creacion', 'name_table', 'quantity_rows_original',
                            'quantity_rows_sql', 'time_load', 'estado'] ]
    # Creación de Lista
    chart = auditoria.values.tolist()
    # Insertar valores
    cursor = connection.cursor()
    cursor.executemany("""INSERT INTO auditoria VALUES (%s,%s,%s,%s,%s,%s,%s)""",chart)
    connection.commit()

if __name__ == "__main__":
    main()
    
