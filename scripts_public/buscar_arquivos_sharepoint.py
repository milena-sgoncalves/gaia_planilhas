import os
import sys
from dotenv import load_dotenv

#carregar .env
load_dotenv()
ROOT = os.getenv('ROOT')

#Definição dos caminhos
CURRENT_DIR = os.path.abspath(os.path.join(ROOT, 'GAIA_copy'))
GAIA_UP = os.path.abspath(os.path.join(ROOT, 'GAIA_up'))
PATH_OFFICE = os.path.abspath(os.path.join(ROOT, 'office365_api'))
SCRIPTS_PUBLIC = os.path.abspath(os.path.join(ROOT, 'scripts_public'))

# Adiciona o diretório correto ao sys.path
sys.path.append(SCRIPTS_PUBLIC)
sys.path.append(PATH_OFFICE)

from download_files import get_files
from scripts_public.apagar_arquivos_pasta import apagar_arquivos_pasta


def buscar_arquivos_sharepoint(dt_atualizacao_port):
    apagar_arquivos_pasta(CURRENT_DIR)
    apagar_arquivos_pasta(GAIA_UP)

    get_files(f"General//Geral//Portfólio Atualizado//{dt_atualizacao_port}", CURRENT_DIR)
    get_files("DWPII", CURRENT_DIR)
    get_files("General//Geral//GAIA", CURRENT_DIR)
