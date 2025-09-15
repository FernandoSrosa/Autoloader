import os
import time
import pandas as pd
from sqlalchemy import create_engine
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

class FileHandler(FileSystemEventHandler):
    def __init__(self, process_function):
        self.process_function = process_function

    def on_modified(self, event):
        if not event.is_directory:
            print(f'File modified: {event.src_path}')
            self.process_function(event.src_path)
    
    def on_created(self, event):
        if not event.is_directory:
            print(f'File created: {event.src_path}')
            self.process_function(event.src_path)

class FolderWatcher:
    def __init__(self, folder: str, process_function):
        self.folder = folder
        self.process_function = process_function
        self.event_handler = FileHandler(self.process_function)
        self.observer = Observer()

    def start(self) -> str:
        self.observer.schedule(self.event_handler, path=self.folder
                               , recursive=False)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()
        return "Watching process terminated"


def process_file(file_path):
    """
    1 - Lê o arquivo (read_data)
    2 - Limpa o arquivo (data_cleaning)
    3 - Pega metados do arquivo (file_stats)
    4 - Verifica se a coluna precisa de criptografia (security)
    5 - Faz upload do arquivo (upload_files)
    6 - Se falhar, faz upload dos metados (upload_files)
    8 - Move arquivo para a pasta "processed" (move_file)
    """
    print(f'Processing file: {file_path}')
    
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
        print(df.head())


def watcher(folder: str) -> str:
    """
    Watches over a folder and process the files accordingly;

    Arguments:
        folder (str): Name of the folder to watch
    Return:
        str: Status of the process 
    """
    folder_watcher = FolderWatcher(folder, process_file)
    return folder_watcher.start()


if __name__ == "__main__":
    folder_to_watch = os.path.join('C  ', os.sep, 'users', 
                        os.getlogin(),'Downloads', 
                        'Autoloader', 'monitored')
    print(watcher(folder_to_watch))
