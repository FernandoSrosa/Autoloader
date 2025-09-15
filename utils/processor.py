from loader import SQLServerUploader
from logger import FileStatsAnalyzer
from datetime import datetime
from shutil import move
from time import sleep
import pandas as pd
import numpy as np
import os

def watcher(folder:str) -> str:
    """
    Watches over a folder and process the files accordling;

    https://pypi.org/project/watchdog/

    Arguments:
        folder (str): Name of the folder to watch
    Return:
        str: Status of the process 
    """
    pass


def read_data(path:str, file:str) -> pd.DataFrame:
    """
    Ensures that the data encoding(ISO-8859-1)

    Arguments:
        path (str): Location of the file to be read
        file (str): Name of the file to be read
    """
    return pd.read_csv(os.path.join(path, file), encoding='ISO-8859-1')



def data_cleaning(df:pd.DataFrame) -> pd.DataFrame:
    """
    Ensures the columns headers are in the correct formatted used in the database
    Ensures that NaN values are null

    Arguments:
        df (DataFrame): Pandas dataframe to be clean
    
    Returns:
        DataFrame: Cleaned DataFrame with the correct format
    """
    df.columns = [x.lower().replace(' ','_').replace('?','').replace('-' ,'_').
                  replace(r'/' , '_').replace('\\' , '_').replace('#' , '_').
                  replace(')' , '_').replace(r'(' , '_').replace('$' , '_') 
                  for x in df.columns]
    df.replace(np.nan, '', regex=True, inplace=True)
    
    return df
 
   



def upload_handler() -> None:
    """
    0 - Monitoria se a pasta contém algum novo arquivo (TODO: watcher)
    1 - Lê o arquivo (read_data)
    2 - Limpa o arquivo (data_cleaning)
    3 - Pega metados do arquivo (file_stats)
    4 - Verifica se a coluna precisa de criptografia (security)
    5 - Faz upload do arquivo (upload_files)
    6 - Se falhar, faz upload dos metados (upload_files)
    8 - Move arquivo para a pasta "processed" (move_file)
    """
    pass

if __name__ == '__main__':
    pass