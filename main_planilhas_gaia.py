from scripts_public.buscar_arquivos_sharepoint import buscar_arquivos_sharepoint
from scripts_public.embrapii_dados import juntar_planilhas
from scripts_public.embrapii_dados import embrapii_dados
from scripts_public.embrapii_dados import combinar_dados
from scripts_public.embrapii_dados import processar_dados_embrapii
from scripts_public.ind_embrapii_dados import calcular_indicadores
from scripts_public.ind_embrapii_dados import processar_dados_ind_embrapii
from scripts_public.embrapii_emp_dados import planilha_empresas
from scripts_public.embrapii_emp_dados import processar_dados_emp
from scripts_public.levar_arquivos_sharepoint import levar_arquivos_sharepoint
from scripts_public.embrapii_dados import excluir_coluna

dt_atualizacao_port = '0726' #data atualizacao portfolio
dt_geracao = '26/07/2024' #data de geracao dos dados
dt_ref = '202406' #data de referencia
dt_ultimo_dia_periodo = "30/06/2024" #data do ultimo dia do periodo de referencia
dt_prim_dia_trim = '01/04/2024' #data do primeiro dia do trimestre de referencia

# embrapii_dados
buscar_arquivos_sharepoint(dt_atualizacao_port)
merged = juntar_planilhas(dt_atualizacao_port)
recorte2 = embrapii_dados(merged, dt_ultimo_dia_periodo)
combinar_dados(recorte2, dt_ref, dt_geracao)
processar_dados_embrapii(dt_ref)

# ind_embrapii
calcular_indicadores(dt_ref, dt_prim_dia_trim, dt_ultimo_dia_periodo)
processar_dados_ind_embrapii(dt_ref)

# embrapii_emp
planilha_empresas(dt_ref, dt_atualizacao_port)
processar_dados_emp(dt_ref)

# retirar coluna CNPJ do embrapii_dados
excluir_coluna('GAIA_UP', f'{dt_ref}_embrapii_dados', 'CNPJs_retirar')

# levar arquivos pro SharePoint
levar_arquivos_sharepoint(dt_ref)