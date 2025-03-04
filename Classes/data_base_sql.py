from time import sleep
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine
from Queries_SQL.query_sql_ponto import PEGAR_INTERVALO, DELETAR_INTERVALO
from Queries_SQL.query_sql_funcionario import DEL_TABELA_FUNCIONARIO
from time import sleep

import os
import pyodbc
from logging import Logger

class DatabaseSQL:
    """
    Classe responsável por gerenciar a conexão com o banco de dados e recuperar intervalos de data.
    """

    def __init__(self):
        """
        Inicializa a classe DatabaseSQL, carregando as credenciais de conexão do arquivo .env.

        Funcionamento:
        --------------
        Utiliza `load_dotenv` para carregar as variáveis de ambiente do arquivo `.env`. Em seguida, monta
        a `string_connection` com as credenciais para conectar ao banco de dados.

        Atributos Definidos:
        --------------------
        string_connection : str
            A string de conexão ao banco de dados, que utiliza o driver, servidor, nome do banco, usuário
            e senha definidos no arquivo `.env`.
        """
        _ = load_dotenv(find_dotenv())
        
        # Carregar as variáveis de credenciais do banco em .env
        self.string_connection = (
            f"DRIVER={os.getenv('DB_DRIVER')};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_DATABASE')};"
            f"UID={os.getenv('DB_UID')};"
            f"PWD={os.getenv('DB_PWD')};"
        )
        #self.logger = logging.getLogger(__name__)

    def parametro_data(self, logger:Logger):
        """
        Obtém o intervalo de datas a partir do banco de dados, com várias tentativas de conexão.

        Funcionamento:
        --------------
        - Realiza até 3 tentativas para conectar ao banco de dados e executar a query `PEGAR_INTERVALO`.
        - Extrai `data_inicial` e `data_final` do banco de dados e ajusta `data_inicial` adicionando um dia.
        - Em caso de erro na conexão, tenta novamente até o número máximo de tentativas, com uma espera de 5 segundos entre elas.

        Retorno:
        -------
        tuple or (None, None)
            Retorna uma tupla com `data_inicial` e `data_final` formatadas como strings (`%Y-%m-%d`) se a conexão
            e a consulta forem bem-sucedidas. Caso contrário, retorna (None, None).

        Exemplo de Uso:
        --------------
        # Obter o intervalo de datas
        data_inicial, data_final = self.parametro_data()
        
        # `data_inicial` e `data_final` contêm o intervalo de datas recuperado, se a operação for bem-sucedida.
        """
        tentativas = 3  # Define o número máximo de tentativas
        tentativa_atual = 0

        while tentativa_atual < tentativas:
            try:
                # Conecta usando 'with' para garantir fechamento
                with pyodbc.connect(self.string_connection, timeout=5) as conexao:
                    logger.info('Conexao realizada para obter os intervalos de data.')

                    with conexao.cursor() as cursor:
                        cursor.execute(PEGAR_INTERVALO)
                        result = cursor.fetchone()
                        data_inicial = result.DataInicial
                        data_final = result.DataFinal
                
                #data_inicial_ajustada = data_inicial + timedelta(days=1)
                
                return data_inicial.strftime('%Y-%m-%d'), data_final.strftime('%Y-%m-%d')

            except pyodbc.Error as e:
                tentativa_atual += 1
                logger.warning(f"Erro na conexao (tentativa {tentativa_atual} de {tentativas}):", e)
                if tentativa_atual < tentativas:
                    logger.warning("Tentando novamente em 5 segundos...")
                    sleep(5)  # Aguarda antes de tentar novamente

        logger.error("Nao foi possível estabelecer conexao apos várias tentativas.")
        return None, None

    
    def carregar_dados_db(self, dataframe, table_name, logger:Logger, funcionario=False):
        """
        Carrega dados de um DataFrame para uma tabela em um banco de dados, com múltiplas tentativas em caso de falha.

        Parâmetros:
        ----------
        dataframe : pandas.DataFrame
            O DataFrame que contém os dados a serem carregados no banco de dados.

        table_name : str
            O nome da tabela de destino no banco de dados.

        funcionario : bool, opcional
            Um indicador para determinar qual operação de deleção deve ser executada antes de inserir os dados:
            - Se `funcionario` for True, executa a query `DEL_TABELA_FUNCIONARIO` para remover dados específicos de funcionários.
            - Se `funcionario` for False, executa a query `DELETAR_INTERVALO` para remover um intervalo padrão de dados.
            O valor padrão é False.

        Funcionamento:
        --------------
        A função estabelece uma conexão com o banco de dados e executa uma das queries de deleção (`DEL_TABELA_FUNCIONARIO` ou `DELETAR_INTERVALO`)
        com base no valor do parâmetro `funcionario`. Após o sucesso da deleção, ela usa o SQLAlchemy para carregar os dados do DataFrame na 
        tabela especificada. Em caso de falha, a função tenta novamente até 3 vezes, com uma espera de 5 segundos entre as tentativas. 
        Caso todas as tentativas falhem, a função exibe uma mensagem de erro final.

        Retorno:
        -------
        None
            A função não retorna nenhum valor, mas imprime mensagens de sucesso ou erro.

        Exemplo de Uso:
        --------------
        # Exemplo de uso da função para carregar dados de funcionários
        self.carregar_dados_db(dataframe=df_funcionarios, table_name="funcionarios", funcionario=True)
        
        # Exemplo de uso da função para carregar dados de pontos (sem funcionários)
        self.carregar_dados_db(dataframe=df_pontos, table_name="pontos", funcionario=False)
        """
        
        tentativas = 3
        tentativa_atual = 0
        engine = None

        while tentativa_atual < tentativas:
            try:
                with pyodbc.connect(self.string_connection) as conexao:
                    with conexao.cursor() as cursor:
                        cursor.execute("SELECT DB_NAME()")
                        db_name = cursor.fetchone()[0]
                        logger.info(f"Conectado ao banco de dados: {db_name}")
                        
                        if funcionario:
                            cursor.execute(DEL_TABELA_FUNCIONARIO)
                            logger.info("Tabela Funcionarios deletada.")
                        else:
                            cursor.execute(DELETAR_INTERVALO)
                            logger.info("Intervalo de dados da folha de pontos deletado com sucesso.")
                        conexao.commit()
                        

                engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(self.string_connection))
                dataframe.to_sql(table_name, engine, if_exists='append', index=False)
                logger.info(f"Dados inseridos na tabela {table_name} com sucesso.")
                return  # Quebra o loop quando a tentativa for de conexão com o banco for ok

            except Exception as e:
                tentativa_atual += 1
                logger.warning(f"Erro ao carregar dados no banco de dados (tentativa {tentativa_atual} de {tentativas}):", e)

                # Se a conexão cair, volta o estado anterior.
                if 'conexao' in locals():
                    conexao.rollback()

                if tentativa_atual < tentativas:
                    logger.warning("Tentando novamente em 5 segundos...")
                    sleep(5)

            finally:
                if engine:
                    engine.dispose()

        logger.error("Não foi possível carregar os dados após várias tentativas.")
