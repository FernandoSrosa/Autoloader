import os
import numpy as np
import pandas as pd

class DataProcessor:
    def __init__(self, pathfile: str = None, path: str = None, file: str = None):
        """
        Provide complete pathfile or path and file separately
        """
        self.path = path
        self.file = file
        self.pathfile = pathfile
        self.df = pd.DataFrame()

    def read_data(self) -> pd.DataFrame:
        """
        Ensures that the data encoding(ISO-8859-1)

        Returns:
            pd.DataFrame: DataFrame with the data read from the file.
        """

        print(self.pathfile)

        if self.pathfile is not None:
            self.df = pd.read_excel(self.pathfile)
        else:
            self.df = pd.read_excel(os.path.join(self.path, self.file))

        return self.df

    def data_cleaning(self) -> pd.DataFrame:
        """
        Ensures the columns headers are in the correct formatted used in the database
        Ensures that NaN values are null

        Returns:
            pd.DataFrame: Cleaned DataFrame with the correct format
        """
        if self.df is None:
            raise ValueError("Data not loaded. Please run read_data() first.")
        
        self.df.columns = [x.lower().replace(' ','_').replace('?','').replace('-' ,'_').
                          replace(r'/' , '_').replace('\\' , '_').replace('#' , '_').
                          replace(')' , '_').replace(r'(' , '_').replace('$' , '_') 
                          for x in self.df.columns]
        self.df.replace(np.nan, '', regex=True, inplace=True)
        
        return self.df

# Example usage
if __name__ == "__main__":
    path = r"\\10.205.2.10\dump_mis_wfm\watcher_on_going"
    file = r"TBL_DUMMY_UPLOAD$primeiro.xlsx"
    
    processor = DataProcessor(path= path, file= file)
    df = processor.read_data()
    print("Original Data:")
    print(df.head())
    
    cleaned_df = processor.data_cleaning()
    print("Cleaned Data:")
    print(cleaned_df.head())