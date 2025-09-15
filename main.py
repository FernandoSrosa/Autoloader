import os
import time
import logging
import getpass
from datetime import date, datetime
from watchdog.events import FileSystemEvent, LoggingEventHandler, PatternMatchingEventHandler
from watchdog.observers import Observer
from utils.logger import FileStatsAnalyzer
from utils.loader import SQLServerUploader
from utils.cryptography import ColumnEncryptor
from utils.file_processor import DataProcessor
from Cryptodome.Cipher import AES
from utils.move_arquive import move_file
import config as cfg

def comparar_itens_listas(source_list, target_list):
    """
    Compare 2 lists, verify if all itens in source_list are included in target_list.

    It's case insensotove
    """

    print(source_list)
    print(target_list)

    if not target_list:
        print("Nenhuma coluna necessária na tabela")
        result = 1

    else:
        source = set(i.lower() for i in source_list)
        target = set(i.lower() for i in target_list)

        if all(item in target for item in source):
            print("Todos os itens do DF existem no banco")
            result = 1
        else:
            print("DF possui colunas que não existem no banco")
            result = 0

    return result

def exec_on_create_steps(pathfile):

    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    table_name = var1 = var2 = None
    path = os.path.dirname(pathfile)
    file = os.path.basename(pathfile)
    file_raw, extensao = os.path.splitext(file)

    split = file_raw.split("$")
    try:
        table_name = split[0]
        var1 = split[1]
        var2 = split[2]
    except:
        pass

    print("inicial: " + pathfile)
    time.sleep(2)

    ## Processar arquivo csv
    init = DataProcessor(pathfile=pathfile)
    DataProcessor.read_data(init)

    ## Limpar arquivo csv
    df_final = DataProcessor.data_cleaning(init)
    print("Gerado df_final")

    ## Coletar metadados do arquivo csv
    ini_analyzer = FileStatsAnalyzer(path=pathfile)
    df_file = ini_analyzer.file_stats()

    print("aqui")

    df_file["filename"] = file_raw
    df_file["proc_var_01"] = var1
    df_file["proc_var_02"] = var2
    df_file["upload_date"] = start_time
    df_file["table_name"] = table_name

    print("Gerado df_file")

    ## Verificação de criptografia
    encryptor = ColumnEncryptor()
    for column in cfg.SECURITY["encrypt_columns"]:
        if column in df_final.columns:
            encrypted_series = encryptor.encrypt_column(df_final[[column]].squeeze())
            df_final[column] = encrypted_series
    print("Gerado df_encrypted")

    try:
        ## validação de upload
        Verify = SQLServerUploader(user=cfg.DB_WFM_USER, password=cfg.DB_WFM_PWD, server=cfg.DB_WFM_SERVER, database=cfg.DB_WFM_NAME, driver=cfg.DB_WFM_DRIVER)
        tabela_existe = Verify.verify_table_exist(table_name= table_name, schema_name= cfg.DB_WFM_SCHEMA_NAME)

        if tabela_existe == 1:
            print(f"A tabela {table_name} existe no banco de dados. Dados do DF serão inseridos.")

            colunas_existe_db = Verify.verify_table_columns_db(table_name= table_name, schema_name= cfg.DB_WFM_SCHEMA_NAME)
            df_ok = comparar_itens_listas(df_final.columns.tolist(), colunas_existe_db)
            print("df_ok", df_ok)

            if df_ok == 1:
                print("Todas as colunas do arquivo existem na tabela do banco")

                colunas_existem_df = Verify.verify_table_columns_df(table_name= table_name, schema_name= cfg.DB_WFM_SCHEMA_NAME)
                db_ok = comparar_itens_listas(colunas_existem_df, df_final.columns.tolist())
                print("db_ok", db_ok)

                if db_ok == 1:
                    print("Todas as colunas exigidas na tabela existem no arquivo")

                    try:

                        df_final = df_final.astype(str)

                        uploader = SQLServerUploader(user= cfg.DB_WFM_USER, password= cfg.DB_WFM_PWD, server= cfg.DB_WFM_SERVER, database= cfg.DB_WFM_NAME, driver= cfg.DB_WFM_DRIVER)
                        uploader.upload_files(df=df_final, table_name= table_name, schema_name= cfg.DB_WFM_SCHEMA)

                        #Mover arquivo para pasta de sucesso
                        move_file(path, file, cfg.PROCESSED_FOLDER, start_time)
                        #Atualizar Metadados
                        df_file["status_upload"] = 1
                        df_file["desc_upload"] = "Upload realizado com sucesso"

                    except:
                        print(f"A tabela {table_name} não foi carregada devido a incompatibilidade no arquivo. Arquivo será movido para validação.")

                        #Atualizar Metadados
                        df_file["status_upload"] = 0
                        df_file["desc_upload"] = "Falha: arquivo inconsistente com a tabela"
                        #Mover arquivo para pasta de falha
                        move_file(path, file, cfg.VALIDATION_FOLDER, start_time)

                else:
                    df_file["status_upload"] = 0
                    df_file["desc_upload"] = "Falha: colunas exigidas na tabela não existem no arquivo"
                    #Mover arquivo para pasta de falha
                    move_file(path, file, cfg.VALIDATION_FOLDER, start_time)
            else:
                df_file["status_upload"] = 0
                df_file["desc_upload"] = "Falha: arquivo possui colunas que não existem na tabela"
                #Mover arquivo para pasta de falha
                move_file(path, file, cfg.VALIDATION_FOLDER, start_time)
        else:
            df_file["status_upload"] = 0
            df_file["desc_upload"] = "Falha: tabela não existe no banco"
            #Mover arquivo para pasta de falha
            move_file(path, file, cfg.VALIDATION_FOLDER, start_time)

        ## Upload metadados

        print(df_file)

        uploader_metadados = SQLServerUploader(user=cfg.DB_WFM_USER, password=cfg.DB_WFM_PWD, server=cfg.DB_WFM_SERVER, database=cfg.DB_WFM_NAME, driver=cfg.DB_WFM_DRIVER)
        uploader_metadados.upload_files(df=df_file, table_name=cfg.DB_WFM_METADADOS, schema_name=cfg.DB_WFM_SCHEMA)

    except Exception as e:
        print(f"Erro ao processar o arquivo {pathfile}: {e}")

def upload_handler() -> None:
    """
    0 - Monitoria se a pasta contém algum novo arquivo (TODO: watcher) >> na __main__
    1 - Lê o arquivo (read_data)    >> Processar arquivo csv
    2 - Limpa o arquivo (data_cleaning)     >> Limpar arquivo csv
    3 - Pega metados do arquivo (file_stats)        >> Coletar metadados do arquivo csv
    4 - Verifica se a coluna precisa de criptografia (security)     >> Verificação de criptografia
    5 - Faz upload do arquivo (upload_files)        >> Upload do dataframe
    6 - Se falhar, faz upload dos metados (upload_files)
    8 - Move arquivo para a pasta "processed" (move_file)       >> Mover arquivo
    """
    pass

class Handler(PatternMatchingEventHandler):
    def __init__(self) -> None:
        super().__init__(patterns=['*.xlsx'], ignore_directories=True, case_sensitive=False)

    def on_any_event(self, event: FileSystemEvent) -> None:
        return super().on_created(event)

    def on_created(self, event: FileSystemEvent) -> None:
        time.sleep(2)
        pathfile = fr"{event.src_path}"
        try:
            exec_on_create_steps(pathfile=pathfile)
        except:
            print(f"Erro ao processar o arquivo {pathfile}")

        return super().on_created(event)

    def on_modified(self, event: FileSystemEvent) -> None:
        return super().on_modified(event)

    def on_deleted(self, event: FileSystemEvent) -> None:
        print("Deletado")
        return super().on_deleted(event)

print("pass")

if __name__ == "__main__":
    data_hoje = date.today()
    user = getpass.getuser()

    logging.basicConfig(level=logging.INFO,
                        filename=f'log_{data_hoje}.log', filemode='a',
                        format='%(asctime)s | %(process)d | %(message)s' + f' | user: {user}' + ' | %(name)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    path = cfg.MONITORED_FOLDER
    logging.info(f'start watching directory {path!r}')

    ## Para execução das ações
    event_handler_execute = Handler()
    observer_execute = Observer()
    observer_execute.schedule(event_handler_execute, path, recursive=True)
    observer_execute.start()

    ## Para gerar o LOG
    event_handler_log = LoggingEventHandler()
    observer_log = Observer()
    observer_log.schedule(event_handler_log, path, recursive=True)
    observer_log.start()

    try:
        while True:
            time.sleep(1)
    finally:
        observer_log.stop()
        observer_log.join()
        observer_execute.stop()
        observer_execute.join()

