import os
import sys
import math
import pandas as pd
from threading import Thread
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.sql import delete
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

sys.path.append(os.getcwd())

class SQLServerUploader:
    def __init__(self, user: str, password: str, server: str, database: str, driver: str):
        self.connection_string = self._create_connection_string(user, password, server, database, driver)
        self.engine = create_engine(self.connection_string)
        self.database = database

    def _create_connection_string(self, user: str, password: str, server: str, database: str, driver: str) -> str:
        return f"mssql+pyodbc://{user}:{password}@{server}:1433/{database}?driver={driver}"
    
    def verify_table_exist(self, table_name: str, schema_name: str) -> int:
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(f"""
                SELECT 1
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_CATALOG = '{self.database}'
                AND TABLE_SCHEMA = '{schema_name}'
                AND TABLE_NAME = '{table_name}'
                """)).fetchone()

                print(result)
                return 1 if result else 0

        except SQLAlchemyError as e:
            print(f"Erro ao verificar a tabela: {e}")
            return 0
    
    def verify_table_columns_db(self, table_name: str, schema_name: str) -> int:
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_CATALOG = '{self.database}'
                AND TABLE_SCHEMA = '{schema_name}'
                AND TABLE_NAME = '{table_name}'
                """)).fetchall()

                list_result = result
                list_column_db = []

                for i in list_result:
                    list_column_db.append(i[0])
                    
                return list_column_db

        except SQLAlchemyError as e:
            print(f"Erro ao verificar a tabela: {e}")
            return 0

    def verify_table_columns_df(self, table_name: str, schema_name: str) -> int:
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(f"""
                SELECT NAME
                FROM SYS.COLUMNS 
                WHERE OBJECT_ID = OBJECT_ID('[{self.database}].[{schema_name}].[{table_name}]')
                AND IS_NULLABLE = 0 AND IS_IDENTITY = 0 AND IS_COMPUTED = 0
                """)).fetchall()

                list_result = result
                list_column_db = []

                for i in list_result:
                    list_column_db.append(i[0])

                return list_column_db

        except SQLAlchemyError as e:
            print(f"Erro ao verificar a tabela: {e}")
            return 0

    def upload_chunk(self, chunk: pd.DataFrame, table_name: str, schema_name: str, if_exists: str = 'append') -> str:

        with self.engine.begin() as conn:
            if table_name == 'tblAuthors':
                conn.execute(text(f"SET IDENTITY_INSERT {table_name} ON"))
                chunk.to_sql(name=table_name, schema=schema_name, con=conn, if_exists=if_exists, index=False)
                conn.execute(text(f"SET IDENTITY_INSERT {table_name} OFF"))
            else:
                chunk.to_sql(name=table_name, schema=schema_name, con=conn, if_exists=if_exists, index=False)

        conn.close()

        return "Chunk uploaded"

    def upload_files(self, df: pd.DataFrame, table_name: str, schema_name: str, if_exists: str = 'append', num_threads: int = 1) -> str:
        chunk_size = math.ceil(len(df) / num_threads)
        chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)] if chunk_size > 0 else ([df] if not df.empty else [])

        threads = []
        for chunk in chunks:
            thread = Thread(target=self.upload_chunk, args=(chunk, table_name, schema_name, if_exists))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        return f"All data was uploaded to {table_name}"

    def update_table(self, table_name: str, set_clause: str, where_clause: str) -> str:
        query = text(f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}")
        with self.engine.connect() as connection:
            connection.execute(query)
            connection.commit()
            connection.close()
        return "Table updated"
    
    def insert_table(self, table_name: str, collumns: str, values: str) -> str:
        query = text(f"INSERT INTO {table_name} ({collumns}) VALUES ({values})")
        with self.engine.connect() as connection:
            connection.execute(query)
            connection.commit()
            connection.close()
        return "Table updated"
    
    def run_stored_procedure(self, procedure_name: str, parameters: dict = None) -> str:
        if parameters:
            param_placeholders = ", ".join([f"{k} = '{v}'" for k, v in parameters.items()])
            query = text(f"EXEC {procedure_name} {param_placeholders} ")
        else:
            query = text(f"EXEC {procedure_name} ")

        with self.engine.connect() as connection:
            connection.execute(query)
            connection.commit()
            connection.close()
        return "Stored procedure executed"
    
    def return_run_stored_procedure(self, procedure_name: str, parameters: dict = None) -> str:
        if parameters:
            param_placeholders = ", ".join([f"@{k} = '{v}'" for k, v in parameters.items()])
            query = text(f"""
                            DECLARE @return INT; 
                            EXEC {procedure_name} {param_placeholders} , @max_id = @return Output; 
                            SELECT @return as max_id;
                            """)
        else:
            query = text(f"EXEC {procedure_name} ")

        with self.engine.connect() as connection:
            result_proxy = connection.execute(query)    # Executa a stored procedure
            result = result_proxy.fetchall()            # Captura todos os resultados
            result_proxy.close()
            connection.commit()
            connection.close()

        return "Stored procedure executed", result

    def run_select_return(self, table_name: str, schema_name: str, statement: str, parameters: dict = None) -> str:
        
        if parameters:
            query = text(f"SELECT {statement} FROM {schema_name}.{table_name} where 1 = 1 and {parameters}")
        else:
            query = text(f"SELECT {statement} FROM {schema_name}.{table_name} where 1 = 1")

        with self.engine.connect() as connection:
            result_proxy = connection.execute(query)    # Executa a stored procedure
            result = result_proxy.fetchall()            # Captura todos os resultados
            result_proxy.close() 
            connection.commit()
            connection.close()

        return "Select executed", result

    def delete_table(self, table_name: str, schema_name: str, parameters: str = "1 = 1") -> str:
        query = text(f"DELETE FROM {schema_name}.{table_name} WHERE {parameters}")
        with self.engine.connect() as connection:
            connection.execute(query)
            connection.commit()
            connection.close()
        return "Table deleted"

if __name__ == "__main__":
    pass
