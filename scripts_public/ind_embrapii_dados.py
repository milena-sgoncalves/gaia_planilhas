import os
import sys
import pandas as pd
from dotenv import load_dotenv

#carregar .env
load_dotenv()
ROOT = os.getenv('ROOT')

#sys.path
SCRIPTS_PUBLIC = os.path.abspath(os.path.join(ROOT, 'scripts_public'))
GAIA_COPY = os.path.abspath(os.path.join(ROOT, 'GAIA_copy'))
GAIA_UP = os.path.abspath(os.path.join(ROOT, 'GAIA_up'))
sys.path.append(SCRIPTS_PUBLIC)

from processar_excel import processar_excel

def calcular_indicadores(dt_ref, dt_prim_dia_trim, dt_ultimo_dia_periodo):
    # transformando em data
    dt_ultimo_dia_periodo = pd.to_datetime(dt_ultimo_dia_periodo, format = '%d/%m/%Y', errors = 'coerce')
    dt_prim_dia_trim = pd.to_datetime(dt_prim_dia_trim, format = '%d/%m/%Y', errors = 'coerce')

    # lendo a planilha
    embrapii_dados = pd.read_excel(os.path.abspath(os.path.join(GAIA_UP, f'{dt_ref}_embrapii_dados.xlsx')))
    pedidos_pi = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'pedidos_pi.xlsx')))
    ppi_recorte = pedidos_pi[pedidos_pi['data_pedido'] <= dt_ultimo_dia_periodo]

    # fazendo recorte trimestral
    recorte_trim = embrapii_dados[embrapii_dados['embrapii_10_dt_contrato'] >= dt_prim_dia_trim]
    recorte_ppi_trim = ppi_recorte[ppi_recorte['data_pedido'] >= dt_prim_dia_trim]

    # empresas
    emp = embrapii_dados.assign(CNPJs_retirar=embrapii_dados['CNPJs_retirar'].str.split('; ')).explode('CNPJs_retirar')
    emp_recorte = recorte_trim.assign(CNPJs_retirar=embrapii_dados['CNPJs_retirar'].str.split('; ')).explode('CNPJs_retirar')

    # projetos
    contagem_linhas = embrapii_dados.groupby('embrapii_02_nome_ict').size().reset_index(name = 'ind_embrapii_03_proj_contrat_acum')
    contagem_linhas_trim = recorte_trim.groupby('embrapii_02_nome_ict').size().reset_index(name = 'ind_embrapii_04_proj_contrat_trim')

    # calculando indicadores acumulados
    ind_acum = embrapii_dados.groupby('embrapii_02_nome_ict').agg({
        'dt_ref': 'first',
        'dt_geracao': 'first',
        'cod_ibge': 'first',
        'nome': 'first',
        'uf': 'first',
        'embrapii_03_tipo_ict': 'first',
        'embrapii_15_val_total': 'sum',
        'embrapii_16_val_aporte_embrapii': 'sum'
    }).reset_index()

    # calculando indicadores trimestrais
    ind_trim = recorte_trim.groupby('embrapii_02_nome_ict').agg({
        'embrapii_15_val_total': 'sum',
        'embrapii_16_val_aporte_embrapii': 'sum'
    }).reset_index()

    # calculando indicadores de num de empresas
    emp_acum = emp.groupby('embrapii_02_nome_ict')['CNPJs_retirar'].nunique()
    emp_trim = emp_recorte.groupby('embrapii_02_nome_ict')['CNPJs_retirar'].nunique()

    # calculando indicadores de pedidos de pi
    ppi_acum = ppi_recorte.groupby('unidade_embrapii').size().reset_index(name = 'ind_embrapii_11_prop_intelec_acum')
    ppi_trim = recorte_ppi_trim.groupby('unidade_embrapii').size().reset_index(name = 'ind_embrapii_12_prop_intelec_trim')

    # juntandos os indicadores
    indicadores = ind_acum.merge(contagem_linhas, on = 'embrapii_02_nome_ict', how = 'left')
    indicadores = indicadores.merge(contagem_linhas_trim, on = 'embrapii_02_nome_ict', how = 'left')
    indicadores = indicadores.merge(ind_trim, on = 'embrapii_02_nome_ict', how = 'left')
    indicadores = indicadores.merge(emp_acum, on = 'embrapii_02_nome_ict', how = 'left')
    indicadores = indicadores.merge(emp_trim, on = 'embrapii_02_nome_ict', how = 'left')
    indicadores = indicadores.merge(ppi_acum, left_on = 'embrapii_02_nome_ict', right_on = 'unidade_embrapii', how = 'left')
    indicadores = indicadores.merge(ppi_trim, left_on = 'embrapii_02_nome_ict', right_on = 'unidade_embrapii', how = 'left')

    indicadores = indicadores.fillna(0)

    indicadores.to_excel(os.path.abspath(os.path.join(GAIA_COPY, 'indicadores.xlsx')), index = False)


def processar_dados_ind_embrapii(dt_ref):
    # Definições dos caminhos e nomes de arquivos
    origem = os.path.join(ROOT, 'GAIA_copy')
    destino = os.path.join(ROOT, 'GAIA_up')
    nome_arquivo = "indicadores.xlsx"
    arquivo_origem = os.path.join(origem, nome_arquivo)
    arquivo_destino = os.path.join(destino, f'{dt_ref}_ind_embrapii_dados.xlsx')

    # Campos de interesse e novos nomes das colunas
    campos_interesse = [
        'dt_ref',
        'dt_geracao',
        'cod_ibge',
        'nome',
        'uf',
        'embrapii_02_nome_ict',
        'embrapii_03_tipo_ict',
        'ind_embrapii_03_proj_contrat_acum',
        'ind_embrapii_04_proj_contrat_trim',
        'embrapii_15_val_total_x',
        'embrapii_15_val_total_y',
        'embrapii_16_val_aporte_embrapii_x',
        'embrapii_16_val_aporte_embrapii_y',
        'CNPJs_retirar_x',
        'CNPJs_retirar_y',
        'ind_embrapii_11_prop_intelec_acum',
        'ind_embrapii_12_prop_intelec_trim',
    ]

    novos_nomes_e_ordem = {
        'dt_ref': 'dt_ref',
        'dt_geracao': 'dt_geracao',
        'cod_ibge': 'cod_ibge',
        'nome': 'nome',
        'uf': 'uf',
        'embrapii_02_nome_ict': 'ind_embrapii_01_nome_ict',
        'embrapii_03_tipo_ict': 'ind_embrapii_02_tipo_ict',
        'ind_embrapii_03_proj_contrat_acum': 'ind_embrapii_03_proj_contrat_acum',
        'ind_embrapii_04_proj_contrat_trim': 'ind_embrapii_04_proj_contrat_trim',
        'embrapii_15_val_total_x': 'ind_embrapii_05_valor_proj_acum',
        'embrapii_15_val_total_y': 'ind_embrapii_06_valor_proj_trim',
        'embrapii_16_val_aporte_embrapii_x': 'ind_embrapii_07_valor_emb_acum',
        'embrapii_16_val_aporte_embrapii_y': 'ind_embrapii_08_valor_emb_trim',
        'CNPJs_retirar_x': 'ind_embrapii_09_emp_contrat_acum',
        'CNPJs_retirar_y': 'ind_embrapii_10_emp_contrat_trim',
        'ind_embrapii_11_prop_intelec_acum': 'ind_embrapii_11_prop_intelec_acum',
        'ind_embrapii_12_prop_intelec_trim': 'ind_embrapii_12_prop_intelec_trim'
    }

    # Campos de data e valor
    campos_data = []
    campos_valor = ['ind_embrapii_05_valor_proj_acum', 'ind_embrapii_06_valor_proj_trim',
                    'ind_embrapii_07_valor_emb_acum', 'ind_embrapii_08_valor_emb_trim']


    processar_excel(arquivo_origem, campos_interesse, novos_nomes_e_ordem, arquivo_destino, campos_data, campos_valor)