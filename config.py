import os
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.append("..")

ROOT_FOLDER = f"\\10.205.2.10\dump_mis_wfm\testedev"
MONITORED_FOLDER=  r"\\10.205.2.10\dump_mis_wfm\testedev"
PROCESSED_FOLDER = r"\\10.205.2.10\dump_mis_wfm\watcher_processed"
VALIDATION_FOLDER = r"\\10.205.2.10\dump_mis_wfm\watcher_validation"

#ROOT_FOLDER = r"D:\PROJETOS_PYTHON\Watcher"
#MONITORED_FOLDER=  r"D:\PROJETOS_PYTHON\Watcher"
#PROCESSED_FOLDER = r"D:\PROJETOS_PYTHON\Watcher"
#VALIDATION_FOLDER = r"D:\PROJETOS_PYTHON\Watcher"

ENCODING = os.getenv('ENCODING')

SECURITY = {
    "key": os.getenv('ENCRYPTION_KEY'),
    "initial_vector": int(os.getenv('INITIAL_VECTOR')),
    "encrypt_columns": ["cpf", "cnpj"]
    }

# DATABASE WFM
DB_WFM_USER = os.getenv("DB_USER")
DB_WFM_PWD = os.getenv("DB_PWD")
DB_WFM_SERVER = os.getenv("DB_SERVER")
DB_WFM_NAME = os.getenv("DB_NAME")
DB_WFM_DIALECT = os.getenv("DB_DIALECT")
DB_WFM_DRIVER = os.getenv("DB_DRIVER")
DB_WFM_TABLE = os.getenv("DB_WFM_TABLE")
DB_WFM_SCHEMA_NAME = os.getenv("DB_SCHEMA")
DB_WFM_TABLE = os.getenv("DB_TABLE")
DB_WFM_METADADOS = os.getenv("DB_TABLE_METADADOS")

DB_WFM_SCHEMA = "[" + DB_WFM_SCHEMA_NAME + "]"