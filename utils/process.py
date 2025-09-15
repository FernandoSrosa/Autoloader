import pandas as pd

def read_data(path:str, file:str) -> pd.DataFrame:
    """
    Ensures that the data encoding(ISO-8859-1)

    Arguments:
        path (str): Location of the file to be read
        file (str): Name of the file to be read
    """
    return pd.read_csv(os.path.join(path, file), encoding='ISO-8859-1')

