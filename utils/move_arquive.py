import os
from datetime import datetime
import shutil

def move_file(caminho_origem, arquivo_origem, caminho_destino, horario):

    horario = horario.replace("/", "_").replace(":", "-")

    file_raw, extensao = os.path.splitext(arquivo_origem)
    new_file_name = f"{file_raw}_{horario}{extensao}"
    original_name = os.path.join(caminho_origem, arquivo_origem)
    renamed_name = os.path.join(caminho_destino, new_file_name)
    
    if not os.path.exists(caminho_destino):
        os.makedirs(caminho_destino)

    try:
        os.rename(original_name, renamed_name)
        print(f'Arquivo movido para {caminho_destino}')
    except Exception as e:
        print(f'Erro ao mover o arquivo: {e}')

if __name__ == "__main__":
    pass