from Classes.data_api import DataAPI
from Classes.data_base_sql import DatabaseSQL
from _Utilidades.columns_json import colunas_dfuncionarios, colunas_folhaPonto
from Queries_SQL.query_sql_funcionario import TABELA_FUNCIONARIO
from Queries_SQL.query_sql_ponto import TABELA_PONTO
from datetime import datetime
from _Utilidades.utilities import setup_logger
import logging
import json
if __name__ == '__main__':
    logger = setup_logger(log_file="log.log", log_level=logging.INFO)    # Chamamdo minhas classes

    #Chamada das Classes
    conexao = DatabaseSQL()
    dados = DataAPI()
    ## -------------------------

    token = dados.get_api_token(logger=logger)
    banco_id = dados.get_api_bancos(token=token, logger=logger)

    data_inicial_ajustada, data_fim = conexao.parametro_data(logger=logger)
    
    data_fim = datetime.now().strftime('%Y-%m-%d')
    #df_funcionarios = dados.get_funcionarios(banco_id=banco_id, logger=logger)

    # data_inicio = "2024-09-29"
    # data_fim = "2024-11-07"

    df_pontos = dados.get_pontos(token=token, id_banco=banco_id, data_inicio=data_inicial_ajustada, data_fim=data_fim, logger=logger)
    caminho_json_api = r"C:\Users\Hélder\OneDrive - LFG Tech\Documentos\VSCODE\api_burgues\API Burgues\ponto_e_funcionario\chamada_api_pontos.json"

    # Salvando o retorno da API como JSON
    with open(caminho_json_api, "w", encoding="utf-8") as arquivo_json:
        json.dump(df_pontos, arquivo_json, ensure_ascii=False, indent=1)

    print(f"JSON salvo em: {caminho_json_api}")
    #raw_df1 = dados.etl_api(data=df_funcionarios, funcao=colunas_dfuncionarios, logger=logger)
    # raw_df2 = dados.etl_api(data=df_pontos, funcao=colunas_folhaPonto, logger=logger)

    # #final_df1 = dados.etl_dataframe(raw_df1, logger=logger)
    # final_df2 = dados.etl_dataframe(raw_df2, logger=logger, plus=False)
    # final_df2.to_excel('json_folha.xlsx', index=False)

    #alimentar_tabela_funcionario = conexao.carregar_dados_db(final_df1, table_name=TABELA_FUNCIONARIO, logger=logger, funcionario=True)
    #alimentar_tabela_ponto = conexao.carregar_dados_db(dataframe=final_df2, table_name=TABELA_PONTO, logger=logger)
