import os
import pandas as pd
from subprocess import Popen, PIPE
from collections import namedtuple

class FileStatsAnalyzer:
    def __init__(self, path: str):
        self.path = path

    @staticmethod
    def sliceit(iterable: str, tup: tuple) -> str:
        return iterable[tup[0]:tup[1]].strip()

    @staticmethod
    def convert_cat(line: str) -> pd.DataFrame:
        """
        Returns a DataFrame with all the necessary information to access who created the file
        """

        Stat = namedtuple('Stat', 'date time size owner')
        col_names = ['date', 'time', 'size', 'owner']  #, 'filename'
        stat_index = Stat(date=(0, 11), 
                          time=(11, 21), 
                          size=(27, 59), 
                          owner=(35, 59), 
                          #filename=(59, 99999)
                          )

        stat = Stat(date=FileStatsAnalyzer.sliceit(line, stat_index.date),
                          time=FileStatsAnalyzer.sliceit(line, stat_index.time),
                          size=FileStatsAnalyzer.sliceit(line, stat_index.size).split(" ")[0],
                          owner=FileStatsAnalyzer.sliceit(line, stat_index.owner)[-8:],
                          #filename=FileStatsAnalyzer.sliceit(line, stat_index.filename)
                          )

        df = pd.DataFrame(stat).transpose()
        df.columns = col_names

        print(df.columns)
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')

        return df

    def file_stats(self) -> pd.DataFrame:
        """
        Check if the file or path is valid and creates an instance to run the analysis

        Returns:
            pd.DataFrame: With the metadata of the file.
        """
        
        if not os.path.isdir(self.path):
            dirname, filename = os.path.split(self.path)
        else:
            dirname = self.path

        cmd = ["cmd", "/c", "dir", dirname, "/q"]
        session = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        result = session.communicate()[0].decode('cp1252')

        if os.path.isdir(self.path):
            line = result.splitlines()[5]
            return self.convert_cat(line)
        else:
            for line in result.splitlines()[5:]:
                if filename in line:
                    return self.convert_cat(line)
            else:
                raise Exception('Could not locate file')

if __name__ == '__main__':
    path = os.path.join('C  ', os.sep, 'users', 
                        os.getlogin(), 'Downloads', 'requirements.txt')
    
    path = r"C:\Users\fsant063\Documents\Em Edição\01 - DEV\01 - PROJECT WATCHER\Autoloader v2\TBL_DUMMY_UPLOAD.csv"
    print(os.path.isdir(path))

    dirname, filename = os.path.split(path)
    print(dirname)
    print(filename)

    analyzer = FileStatsAnalyzer(path)
    ret = analyzer.file_stats()

    ret["status_upload"] = 0

    print(ret)
    print(type(ret))