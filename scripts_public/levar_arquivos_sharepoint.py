import os
import sys
from dotenv import load_dotenv

#carregar .env
load_dotenv()
ROOT = os.getenv('ROOT')

#Definição dos caminhos
GAIA_COPY = os.path.abspath(os.path.join(ROOT, 'GAIA_copy'))
GAIA_UP = os.path.abspath(os.path.join(ROOT, 'GAIA_up'))
PATH_OFFICE = os.path.abspath(os.path.join(ROOT, 'office365_api'))
SCRIPTS_PUBLIC = os.path.abspath(os.path.join(ROOT, 'scripts_public'))

# Adiciona o diretório correto ao sys.path
sys.path.append(SCRIPTS_PUBLIC)
sys.path.append(PATH_OFFICE)

from upload_files import upload_files

def levar_arquivos_sharepoint(dt_ref):

    upload_files(GAIA_UP, f"General//Geral//GAIA//{dt_ref}")
