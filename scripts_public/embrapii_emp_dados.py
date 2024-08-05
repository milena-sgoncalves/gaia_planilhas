import os
import sys
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from processar_excel import processar_excel

#carregar .env
load_dotenv()
ROOT = os.getenv('ROOT')

#sys.path
SCRIPTS_PUBLIC = os.path.abspath(os.path.join(ROOT, 'scripts_public'))
GAIA_COPY = os.path.abspath(os.path.join(ROOT, 'GAIA_copy'))
GAIA_UP = os.path.abspath(os.path.join(ROOT, 'GAIA_up'))
sys.path.append(SCRIPTS_PUBLIC)

def planilha_empresas(dt_ref, dt_atualizacao_port):
    # lendo a planilha
    embrapii_dados = pd.read_excel(os.path.abspath(os.path.join(GAIA_UP, f'{dt_ref}_embrapii_dados.xlsx')))
    emp = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, f'Portfolio Trabalho 2024{dt_atualizacao_port}.xlsx')), sheet_name = 'Informações Empresas')
    
    # duplicando projetos com mais de uma empresa
    proj_emp = embrapii_dados.assign(CNPJs_retirar=embrapii_dados['CNPJs_retirar'].str.split('; ')).explode('CNPJs_retirar')

    # juntando com as informacoes das empresas
    merged = proj_emp.merge(emp, left_on = 'CNPJs_retirar', right_on = 'CNPJ', how = 'left')

    # calcular o num de empresas para cada projeto
    empresas_no_projeto = merged.groupby('embrapii_01_cod_projeto').size().reset_index(name = 'embrapii_emp_13_num_empresas_projeto')

    # calcular o num de projetos que cada empresa possui
    num_projetos = merged.groupby('CNPJs_retirar').size().reset_index(name = 'embrapii_emp_05_num_proj_empresa')

    # juntando com a planilha
    merged = merged.merge(empresas_no_projeto, on = 'embrapii_01_cod_projeto', how = 'left')
    merged = merged.merge(num_projetos, on = 'CNPJs_retirar', how = 'left')

    # projeto cooperativo
    merged['embrapii_emp_12_cooperativo'] = np.where(merged['embrapii_emp_13_num_empresas_projeto'] > 1, 'Sim', 'Não')

    merged.to_excel(os.path.abspath(os.path.join(GAIA_COPY, 'embrapii_emp_dados.xlsx')), index = False)


def processar_dados_emp(dt_ref):
    # Definições dos caminhos e nomes de arquivos
    origem = os.path.join(ROOT, 'GAIA_copy')
    destino = os.path.join(ROOT, 'GAIA_up')
    nome_arquivo = "embrapii_emp_dados.xlsx"
    arquivo_origem = os.path.join(origem, nome_arquivo)
    arquivo_destino = os.path.join(destino, f'{dt_ref}_embrapii_emp_dados.xlsx')

    # Campos de interesse e novos nomes das colunas
    campos_interesse = [
        'dt_ref',
        'dt_geracao',
        'Código IBGE Município',
        'Município',
        'Código IBGE UF',
        'CNPJ',
        'Empresa',
        'CNAE Classe',
        'Nomenclatura CNAE Classe',
        'embrapii_emp_05_num_proj_empresa',
        'embrapii_01_cod_projeto',
        'uf',
        'cod_ibge',
        'nome',
        'embrapii_02_nome_ict',
        'embrapii_03_tipo_ict',
        'embrapii_emp_12_cooperativo',
        'embrapii_emp_13_num_empresas_projeto',
        'embrapii_09_titulo_projeto',
        'embrapii_10_dt_contrato',
        'embrapii_11_dt_final',
        'embrapii_12_area_aplicacao',
        'embrapii_13_tecnologia_habilitadora',
        'embrapii_14_missao_cndi',
        'embrapii_15_val_total',
        'embrapii_16_val_aporte_embrapii',
        'embrapii_17_pedidos_pi',
    ]

    novos_nomes_e_ordem = {
        'dt_ref': 'dt_ref',
        'dt_geracao': 'dt_geracao',
        'Código IBGE Município': 'cod_ibge',
        'Município': 'nome',
        'Código IBGE UF': 'uf',
        'CNPJ': 'embrapii_emp_01_cnpj_empresa',
        'Empresa': 'embrapii_emp_02_nome_empresa',
        'CNAE Classe': 'embrapii_emp_03_cnae_empresa',
        'Nomenclatura CNAE Classe': 'embrapii_emp_04_nome_cnae_empresa',
        'embrapii_emp_05_num_proj_empresa': 'embrapii_emp_05_num_proj_empresa',
        'embrapii_01_cod_projeto': 'embrapii_emp_06_cod_proj',
        'uf': 'embrapii_emp_07_uf_ict',
        'cod_ibge': 'embrapii_emp_08_cod_ibge_ict',
        'nome': 'embrapii_emp_09_nome_municipio_ict',
        'embrapii_02_nome_ict': 'embrapii_emp_10_nome_ict',
        'embrapii_03_tipo_ict': 'embrapii_emp_11_tipo_ict',
        'embrapii_emp_12_cooperativo': 'embrapii_emp_12_cooperativo',
        'embrapii_emp_13_num_empresas_projeto': 'embrapii_emp_13_num_empresas_projeto',
        'embrapii_09_titulo_projeto': 'embrapii_emp_14_titulo_projeto',
        'embrapii_10_dt_contrato': 'embrapii_emp_15_dt_contrato',
        'embrapii_11_dt_final': 'embrapii_emp_16_dt_final',
        'embrapii_12_area_aplicacao': 'embrapii_emp_17_area_aplicacao',
        'embrapii_13_tecnologia_habilitadora': 'embrapii_emp_18_tecnologia_habilitadora',
        'embrapii_14_missao_cndi': 'embrapii_emp_19_missao_cndi',
        'embrapii_15_val_total': 'embrapii_emp_20_val_total',
        'embrapii_16_val_aporte_embrapii': 'embrapii_emp_21_val_aporte_embrapii',
        'embrapii_17_pedidos_pi': 'embrapii_emp_22_pedidos_pi'
    }

    # Campos de data e valor
    campos_data = ['dt_geracao', 'embrapii_emp_15_dt_contrato', 'embrapii_emp_16_dt_final']
    campos_valor = ['embrapii_emp_20_val_total', 'embrapii_emp_21_val_aporte_embrapii']

    processar_excel(arquivo_origem, campos_interesse, novos_nomes_e_ordem, arquivo_destino, campos_data, campos_valor)

