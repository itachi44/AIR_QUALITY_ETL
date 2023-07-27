import os
from datetime import datetime
import wbgapi as wb
import pandas as pd
import requests
import pandas as pd
from loguru import logger
from airflow.models import XCom
from settings.params import *
import sys


log_fmt = ("<green>{time:YYYY-MM-DD HH:mm:ss.SSS!UTC}</green> | <level>{level: <8}</level> | "
           "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - {message}"
          )
log_config = {
    "handlers": [
        {"sink": sys.stderr, "format": log_fmt},
    ],
}
logger.configure(**log_config)

def preprocess_numerical_data_by_mean(values: pd.DataFrame):
    numerical_features = values.select_dtypes(include="number").columns
    for feature in numerical_features:
        values[feature].fillna(value=values[feature].mean(), inplace=True)


def preprocess_year_data(values: pd.DataFrame, column: str):
    if column in values.columns:
        nv_dates = [date[date.index("YR")+2:] for date in values[column]]
        values[column] = nv_dates
    return values


def create_directory_and_subfolders(path):
    if not os.path.exists(path):
        try:
            # Create the directory and its subfolders
            os.makedirs(path)
            print(f"Directory '{path}' and its subfolders created successfully.")
        except OSError as e:
            print(f"Error: {e}")
    else:
        print(f"Directory '{path}' already exists.")


def extract_wb_data(**kwargs):
    create_directory_and_subfolders('data/input/wb_data/')
    indicators = wb.series.list(q='PM2.5')
    dict_dataframes = {}
    for indicator in indicators:
        series_data = wb.data.DataFrame(indicator["id"], PAYS).transpose()
        data = pd.DataFrame(series_data)
        dict_dataframes.__setitem__(indicator["value"],data)
    task_instance = kwargs['ti']
    task_instance.xcom_push(key='extracted_wb_data', value=dict_dataframes)  # Push data to XCom
    return dict_dataframes


def transform_wb_data(**kwargs):
    #Recuperation des donnees extraites
    task_instance = kwargs['ti']
    extracted_data = task_instance.xcom_pull(task_ids='extract_wb_data', key='extracted_wb_data')
    dict_data = extracted_data

    #Remplacer les donnees manquantes par la moyenne
    for name, dataframe in dict_data.items():
        #Renommer les colonnes de chaque dataframe
        dataframe.rename(columns={'SEN': name[name.index(',')+1:name.index('(')]}, inplace=True)

    dict_dataframe = list(dict_data.values())
    #Regrouper l'ensemble des datarames en un seul en utilisant les index
    dataframe_final = pd.concat(dict_dataframe, axis=1, join="inner")
    #renommer l'index des annees
    dataframe_final.reset_index(drop=False, inplace=True)
    dataframe_final.rename(columns={"index": "annee"}, inplace=True)
    #Pretraitement des donnees numeriques en inferant par la moyenne
    preprocess_numerical_data_by_mean(dataframe_final)
    dataframe_final = preprocess_year_data(dataframe_final, 'annee')
    final_dataframe_json = dataframe_final.to_json(orient='records')
    task_instance = kwargs['ti']
    task_instance.xcom_push(key='transformed_wb_data', value=final_dataframe_json)  # Push data to XCom
    return dataframe_final


def load_wb_data_to_s3(**kwargs):

    task_instance = kwargs['ti']
    transformed_data = task_instance.xcom_pull(task_ids='transform_wb_data', key='transformed_wb_data')
    dataframe_final = pd.read_json(transformed_data, orient='records')

    object_key = 'input/wb_data/wb_data' + str(datetime.now().strftime("%Y-%m-%d%H:%M")) + '.csv'
    file_path = 'data/input/wb_data/wb_data' + str(datetime.now().strftime("%Y-%m-%d%H:%M")) + '.csv'
    dataframe_final.to_csv(file_path, index=False)
    if os.path.exists(file_path):
        return S3.upload_file(file_path, BUCKET_NAME, object_key)
    else:
        raise FileNotFoundError


def extract_api_data(api_url,**kwargs):
    create_directory_and_subfolders('data/input/waqi_data/')
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        print(data)
        task_instance = kwargs['ti']
        task_instance.xcom_push(key='extracted_data', value=data)  # Push data to XCom
        return data
    else:
        print(f"Erreur lors de la requête : {response.status_code}")
        #return None


def transform_api_data(**kwargs):
    task_instance = kwargs['ti']
    extracted_data = task_instance.xcom_pull(task_ids='extract_api_data', key='extracted_data')
    dict_data = extracted_data

    liste_dataset=[]
    #Recuperation des donnees journalieres
    poll_data = dict_data["data"]["forecast"]["daily"]

    #Transformer chaque variable de pollution en un dataframe et renommer les variables
    variables = poll_data.keys()
    for variable in variables:
        data = pd.DataFrame(poll_data[variable])
        for col in data.columns:
            if col !="day":
                data.rename(columns={col: col+"_"+variable}, inplace=True)
        #Les dataframes sont stockes au niveau d'une liste
        liste_dataset.append(data)
    #Les datarames sont regroupes dans un seul en utilisant la jointure sur la colonne day
    result_df = pd.concat([df.set_index('day') for df in liste_dataset], axis=1).reset_index()
    final_dataframe_json = result_df.to_json(orient='records')
    task_instance.xcom_push(key='transformed_data', value=final_dataframe_json)


def load_api_data_to_s3(**kwargs):
    task_instance = kwargs['ti']
    dataframe_json = task_instance.xcom_pull(task_ids='transform_api_data', key='transformed_data')
    dataframe = pd.read_json(dataframe_json, orient='records')
    object_key = 'input/waqi_data/waqi_data'+str(datetime.now().strftime("%Y-%m-%d%H:%M"))+'.csv'
    file_path = 'data/input/waqi_data/waqi_data'+str(datetime.now().strftime("%Y-%m-%d%H:%M"))+'.csv'
    dataframe.to_csv(file_path,index=False)
    if os.path.exists(file_path):
        return S3.upload_file(file_path, BUCKET_NAME, object_key)
    else:
        raise FileNotFoundError



