import os
import sys
import pandas as pd
import numpy as np
from unidecode import unidecode
from dotenv import load_dotenv

#carregar .env
load_dotenv()
ROOT = os.getenv('ROOT')

#sys.path
SCRIPTS_PUBLIC = os.path.abspath(os.path.join(ROOT, 'scripts_public'))
GAIA_COPY = os.path.abspath(os.path.join(ROOT, 'GAIA_copy'))
sys.path.append(SCRIPTS_PUBLIC)

from processar_excel import processar_excel

def juntar_planilhas():
    # lendo as planilhas
    port = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'portfolio.xlsx')))
    proj_emp = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'projetos_empresas.xlsx')))
    emp = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'informacoes_empresas.xlsx')))
    emp['municipio'] = emp['municipio'].astype(str)
    ues = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'info_unidades_embrapii.xlsx')))
    territorial = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'ibge_municipios.xlsx')))
    territorial['cod_municipio_gaia'] = territorial['cod_municipio_gaia'].astype(str)
    cnae = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'cnae_ibge.xlsx')))

    # remover acentos das colunas de municipio
    def remove_acentos(text):
        return unidecode(text)
    
    ues['municipio'] = ues['municipio'].apply(remove_acentos).str.upper()
    ues['municipio'] = np.where(ues['municipio'].str.contains('BARRA DA TIJUCA|BOTAFOGO'), 'RIO DE JANEIRO', ues['municipio'])
    emp['municipio'] = emp['municipio'].apply(remove_acentos).str.upper()
    territorial['no_municipio'] = territorial['no_municipio'].apply(remove_acentos).str.upper()

    #juntando as planilhas
    port_emp = pd.merge(port, proj_emp, on = 'codigo_projeto', how = 'right')
    emp_municipio = pd.merge(emp, territorial, left_on = ['municipio', 'uf'], right_on = ['no_municipio', 'sg_uf'], how = 'left')
    emp_cnae = pd.merge(emp_municipio, cnae, left_on = 'cnae_subclasse', right_on = 'subclasse2', how = 'left')
    emp2 = pd.merge(port_emp, emp_cnae, on = 'cnpj', how = 'left')
    ues2 = pd.merge(ues, territorial, left_on = ['municipio', 'uf'], right_on = ['no_municipio', 'sg_uf'], how = 'left')
    merged = pd.merge(emp2, ues2, on = 'unidade_embrapii', how = 'left')

    emp_cnae.to_excel('empresas.xlsx')

    return merged

def embrapii_dados(merged, dt_ultimo_dia_periodo):
    # transformando em data
    dt_ultimo_dia_periodo = pd.to_datetime(dt_ultimo_dia_periodo, format = '%d/%m/%Y', errors = 'coerce')

    # lendo a planilha pedidos de pi e a planilha geral
    ppi = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'pedidos_pi.xlsx')))
    planilha_geral = merged
    planilha_geral['data_contrato'] = pd.to_datetime(planilha_geral['data_contrato'], format = '%d/%m/%Y', errors = 'coerce')
    ppi['data_pedido'] = pd.to_datetime(ppi['data_pedido'], format = '%d/%m/%Y', errors = 'coerce')

    # fazendo as transformacoes necessarias
    # 1. filtrando os dados para o periodo indicado
    recorte = planilha_geral[planilha_geral['data_contrato'] <= dt_ultimo_dia_periodo]
    ppi_recorte = ppi[ppi['data_pedido'] <= dt_ultimo_dia_periodo]

    # 2. criando coluna de tipo_ict e a coluna valor_total
    recorte['tipo_ict'] = np.where(recorte['tipo_instituicao'].str.contains('Privada|Privado'), 'Privada', 'Pública')
    recorte['valor_total'] = recorte['valor_embrapii'] + recorte['valor_empresa'] + recorte['valor_unidade_embrapii']

    # 3. contando num de pedidos de pi para cada projeto
    contagem_linhas = ppi_recorte.groupby('codigo_projeto').size().reset_index(name = 'embrapii_17_pedidos_pi')

    # 3. juntando recorte e numero de pedidos de pi
    recorte2 = pd.merge(recorte, contagem_linhas, on = 'codigo_projeto', how = 'left')

    return recorte2

def combinar_dados(recorte2, dt_ref, dt_geracao):
     # concatenando os valores de empresas, para ter somente uma linha para cada codigo
    def concat_values(series):
         return '; '.join(series.astype(str))
    
    # agrupando o DataFrame pela coluna 'codigo_projeto'
    combinado = recorte2.groupby('codigo_projeto').agg({
        'cod_municipio_gaia_y': 'first',
        'no_municipio_y': 'first',
        'cod_uf_y': 'first',
        'unidade_embrapii': 'first',
        'tipo_ict': 'first',
        'titulo_publico': 'first',
        'data_contrato': 'first',
        'data_termino': 'first',
        'area_aplicacao': 'first',
        'tecnologia_habilitadora': 'first',
        'missoes_cndi': 'first',
        'valor_total': 'first',
        'valor_embrapii': 'first',
        'cnpj': concat_values,
        'empresa': concat_values,
        'municipio_x': concat_values,
        'cod_municipio_gaia_x': concat_values,
        'porte': concat_values,
        'cnae_subclasse': concat_values,
        'nome_subclasse': concat_values,
        'embrapii_17_pedidos_pi': 'first'
    }).reset_index()
        
    # mesclando com a contagem de linhas
    combinado['dt_ref'] = dt_ref
    combinado['dt_geracao'] = dt_geracao
    combinado['embrapii_17_pedidos_pi'] = combinado['embrapii_17_pedidos_pi'].fillna(0)
    combinado.to_excel(os.path.abspath(os.path.join(GAIA_COPY, 'planilha_combinada.xlsx')), index = False)



def processar_dados_embrapii(dt_ref):
    # Definições dos caminhos e nomes de arquivos
    origem = os.path.join(ROOT, 'GAIA_copy')
    destino = os.path.join(ROOT, 'GAIA_up')
    nome_arquivo = "planilha_combinada.xlsx"
    arquivo_origem = os.path.join(origem, nome_arquivo)
    arquivo_destino = os.path.join(destino, f'{dt_ref}_embrapii_dados.xlsx')

    # Campos de interesse e novos nomes das colunas
    campos_interesse = [
        'dt_ref',
        'dt_geracao',
        'codigo_projeto',
        'cod_municipio_gaia_y',
        'no_municipio_y',
        'cod_uf_y',
        'unidade_embrapii',
        'tipo_ict',
        'titulo_publico',
        'data_contrato',
        'data_termino',
        'area_aplicacao',
        'tecnologia_habilitadora',
        'missoes_cndi',
        'valor_total',
        'valor_embrapii',
        'cnpj',
        'empresa',
        'municipio_x',
        'cod_municipio_gaia_x',
        'porte',
        'cnae_subclasse',
        'nome_subclasse',
        'embrapii_17_pedidos_pi',
    ]

    novos_nomes_e_ordem = {
        'dt_ref': 'dt_ref',
        'dt_geracao': 'dt_geracao',
        'cod_municipio_gaia_y': 'cod_ibge',
        'no_municipio_y': 'nome',
        'cod_uf_y': 'uf',
        'codigo_projeto': 'embrapii_01_cod_projeto',
        'unidade_embrapii': 'embrapii_02_nome_ict',
        'tipo_ict': 'embrapii_03_tipo_ict',
        'cod_municipio_gaia_x': 'embrapii_04_cod_ibge_empresa',
        'municipio_x': 'embrapii_05_nome_municipio_empresa',
        'cnpj': 'CNPJs_retirar',
        'empresa': 'embrapii_06_nome_empresa',
        'cnae_subclasse': 'embrapii_07_cnae_empresa',
        'nome_subclasse': 'embrapii_08_nome_cnae_empresa',
        'titulo_publico': 'embrapii_09_titulo_projeto',
        'data_contrato': 'embrapii_10_dt_contrato',
        'data_termino': 'embrapii_11_dt_final',
        'area_aplicacao': 'embrapii_12_area_aplicacao',
        'tecnologia_habilitadora': 'embrapii_13_tecnologia_habilitadora',
        'missoes_cndi': 'embrapii_14_missao_cndi',
        'valor_total': 'embrapii_15_val_total',
        'valor_embrapii': 'embrapii_16_val_aporte_embrapii',
        'embrapii_17_pedidos_pi': 'embrapii_17_pedidos_pi'
    }

    # Campos de data e valor
    campos_data = ['embrapii_10_dt_contrato', 'embrapii_11_dt_final', 'dt_geracao']
    campos_valor = ['embrapii_15_val_total', 'embrapii_16_val_aporte_embrapii']

    processar_excel(arquivo_origem, campos_interesse, novos_nomes_e_ordem, arquivo_destino, campos_data, campos_valor)

def excluir_coluna(pasta, nome_arquivo, coluna):
    caminho = os.path.join(ROOT, f'{pasta}')

    # lendo a planilha
    planilha = pd.read_excel(os.path.abspath(os.path.join(caminho, f'{nome_arquivo}.xlsx')))

    # retirando a coluna
    planilha.drop(coluna, axis = 1, inplace = True)

    # substituindo a coluna
    planilha.to_excel(os.path.abspath(os.path.join(caminho, f'{nome_arquivo}.xlsx')), index = False)







# def juntar_planilhas():
#     # lendo as planilhas necessarias
#     port = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'portfolio.xlsx')))
#     proj_emp = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'projetos_empresas.xlsx')))
#     emp = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'info_empresas.xlsx')))
#     ues = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'info_unidades_embrapii.xlsx')))
#     clas = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'classificacao_projeto.xlsx')))
#     territorial = pd.read_excel(os.path.abspath(os.path.join(ROOT, 'territorial.xlsx')))

#     # remover acentos das colunas de municipio
#     def remove_acentos(text):
#         return unidecode(text)
    
#     ues['municipio'] = ues['municipio'].apply(remove_acentos).str.upper()
#     territorial['Município'] = territorial['Município'].str.upper()

#     #juntando as planilhas
#     port_clas = pd.merge(port, clas, on = 'codigo_projeto', how = 'right')
#     port_emp = pd.merge(port_clas, proj_emp, on = 'codigo_projeto', how = 'right')
#     emp2 = pd.merge(port_emp, emp, on = 'cnpj', how = 'left')
#     emp3 = pd.merge(emp2, territorial, left_on = ['endereco_municipio', 'endereco_uf'], right_on = ['Município', 'UF'], how = 'left')
#     ue2 = pd.merge(emp3, ues, on = 'unidade_embrapii', how = 'left')
#     merged = pd.merge(ue2, territorial, left_on = ['municipio', 'uf'], right_on = ['Município', 'UF'], how = 'left')

#     # gerando planilha_geral
#     merged.to_excel(os.path.abspath(os.path.join(GAIA_COPY, 'planilha_geral.xlsx')), index = True)


# def combinar_dados():
#     # lendo a planilha geral
#     planilha_geral = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'planilha_geral.xlsx')))

#     # concatenando os valores de empresas, para ter somente uma linha para cada codigo
#     def concat_values(series):
#         return '; '.join(series)
    
#     # agrupando o DataFrame pela coluna 'codigo_projeto'
#     combinado = planilha_geral.groupby('codigo_projeto').agg({
#         'cnpj': concat_values,
#         'razao_social': concat_values,
#         'cnae_principal': concat_values,
#         'cnae_descricao': concat_values,
#         'endereco_uf': concat_values,
#         'endereco_municipio': concat_values,
#         'Código Município_x': concat_values,
#         'Código UF_x': concat_values
#     }).reset_index()

#     # gerando planilha_combinada
#     combinado.to_excel(os.path.abspath(os.path.join(GAIA_COPY, 'planilha_combinada.xlsx')), index = True)