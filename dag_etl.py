from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import warnings
warnings.filterwarnings("ignore")


# General Functions
def list_files_in_folder(service, folder_id):
    try:
        results = service.files().list(q=f"'{folder_id}' in parents").execute()
        files = results.get('files', [])
        return files
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def check_file_existence_in_folder(service, folder_id, file_name):
    # List files in the folder
    files_in_folder = list_files_in_folder(service, folder_id)

    # Check  file in the folder 
    for file_info in files_in_folder:
        if 'name' in file_info and file_info['name'] == file_name:
            print(f"The file with name '{file_name}' exists in the folder.")
            return True

    print(f"The file with name '{file_name}' does not exist in the folder.")
    return False



def download_file_from_drive(drive, file_name, destination_path):
    # Search for the file by name
    file_list = drive.ListFile({'q': f"title='{file_name}'"}).GetList()

    if not file_list:
        return print(f"File with name {file_name} not found in Google Drive.")

    file = file_list[0]
    file.GetContentFile(destination_path)

# Load variables
try:
    # Input variable
    current_script_path = os.path.abspath(__file__)
    main_folder = os.path.dirname(current_script_path)
    folder_config = 'config'
    json_acc = 'key.json'
    config_dir = os.path.join(main_folder,folder_config)

    yesterday = ((datetime.now())-timedelta(1)).strftime('%Y-%m-%d')
    credentials = service_account.Credentials.from_service_account_file(os.path.join(config_dir,json_acc), scopes=['https://www.googleapis.com/auth/drive'])
    service = build('drive', 'v3', credentials=credentials)
except Exception as eror:
    print(eror)


# Function DAG
def     


# Still not finish
    
















