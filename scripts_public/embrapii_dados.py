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

def juntar_planilhas(dt_atualizacao_port):
    # lendo as planilhas
    port = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'portfolio.xlsx')))
    proj_emp = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'projetos_empresas.xlsx')))
    clas = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'classificacao_projeto.xlsx')))
    emp = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, f'Portfolio Trabalho 2024{dt_atualizacao_port}.xlsx')), sheet_name = 'Informações Empresas')
    emp['Código IBGE Município'] = emp['Código IBGE Município'].astype(str)
    ues = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'info_unidades_embrapii.xlsx')))
    territorial = pd.read_excel(os.path.abspath(os.path.join(GAIA_COPY, 'ibge_municipios.xlsx')))
    territorial['cod_municipio_gaia'] = territorial['cod_municipio_gaia'].astype(str)

    # remover acentos das colunas de municipio
    def remove_acentos(text):
        return unidecode(text)
    
    ues['municipio'] = ues['municipio'].apply(remove_acentos).str.upper()
    ues['municipio'] = np.where(ues['municipio'].str.contains('BARRA DA TIJUCA|BOTAFOGO'), 'RIO DE JANEIRO', ues['municipio'])
    territorial['no_municipio'] = territorial['no_municipio'].apply(remove_acentos).str.upper()

    #juntando as planilhas
    port_clas = pd.merge(port, clas, left_on = 'codigo_projeto', right_on = 'Código', how = 'left')
    port_emp = pd.merge(port_clas, proj_emp, on = 'codigo_projeto', how = 'right')
    emp2 = pd.merge(port_emp, emp, left_on = 'cnpj', right_on = 'CNPJ', how = 'left')
    ue2 = pd.merge(emp2, ues, on = 'unidade_embrapii', how = 'left')
    merged = pd.merge(ue2, territorial, left_on = ['municipio', 'uf'], right_on = ['no_municipio', 'sg_uf'], how = 'left')

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
        'cod_municipio_gaia': 'first',
        'no_municipio': 'first',
        'cod_uf': 'first',
        'unidade_embrapii': 'first',
        'tipo_ict': 'first',
        'titulo_publico': 'first',
        'data_contrato': 'first',
        'data_termino': 'first',
        'Áreas de Aplicação': 'first',
        'Tecnologias Habilitadoras': 'first',
        'Missões - CNDI final': 'first',
        'valor_total': 'first',
        'valor_embrapii': 'first',
        'cnpj': concat_values,
        'Empresa': concat_values,
        'Município': concat_values,
        'Código IBGE Município': concat_values,
        'Porte': concat_values,
        'CNAE Classe': concat_values,
        'Nomenclatura CNAE Classe': concat_values,
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
        'cod_municipio_gaia',
        'no_municipio',
        'cod_uf',
        'unidade_embrapii',
        'tipo_ict',
        'titulo_publico',
        'data_contrato',
        'data_termino',
        'Áreas de Aplicação',
        'Tecnologias Habilitadoras',
        'Missões - CNDI final',
        'valor_total',
        'valor_embrapii',
        'cnpj',
        'Empresa',
        'Município',
        'Código IBGE Município',
        'Porte',
        'CNAE Classe',
        'Nomenclatura CNAE Classe',
        'embrapii_17_pedidos_pi',
    ]

    novos_nomes_e_ordem = {
        'dt_ref': 'dt_ref',
        'dt_geracao': 'dt_geracao',
        'cod_municipio_gaia': 'cod_ibge',
        'no_municipio': 'nome',
        'cod_uf': 'uf',
        'codigo_projeto': 'embrapii_01_cod_projeto',
        'unidade_embrapii': 'embrapii_02_nome_ict',
        'tipo_ict': 'embrapii_03_tipo_ict',
        'Código IBGE Município': 'embrapii_04_cod_ibge_empresa',
        'Município': 'embrapii_05_nome_municipio_empresa',
        'cnpj': 'CNPJs_retirar',
        'Empresa': 'embrapii_06_nome_empresa',
        'CNAE Classe': 'embrapii_07_cnae_empresa',
        'Nomenclatura CNAE Classe': 'embrapii_08_nome_cnae_empresa',
        'titulo_publico': 'embrapii_09_titulo_projeto',
        'data_contrato': 'embrapii_10_dt_contrato',
        'data_termino': 'embrapii_11_dt_final',
        'Áreas de Aplicação': 'embrapii_12_area_aplicacao',
        'Tecnologias Habilitadoras': 'embrapii_13_tecnologia_habilitadora',
        'Missões - CNDI final': 'embrapii_14_missao_cndi',
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