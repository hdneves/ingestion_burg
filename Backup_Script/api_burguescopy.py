from dotenv import load_dotenv, find_dotenv
from pandas import json_normalize
from columns_json import colunas_dfuncionarios, colunas_folhaPonto
from sqlalchemy import create_engine
from query_sql_ponto import PEGAR_INTERVALO, DELETAR_INTERVALO, TABELA_PONTO
from query_sql_funcionario import DEL_TABELA_FUNCIONARIO, TABELA_FUNCIONARIO
from time import sleep
from datetime import datetime, timedelta

import pandas as pd
import requests
import os
import pyodbc

_ = load_dotenv(find_dotenv())

class PontoWebAPI:

    def __init__(self):
        """
        Inicializa uma instância da classe, carregando variáveis de ambiente e URLs necessárias para autenticação na API.

        Atributos:
        ----------
        grant_type : str
            O tipo de concessão utilizado na autenticação da API, obtido do arquivo `.env`.
        
        username : str
            O nome de usuário para autenticação, obtido do arquivo `.env`.
        
        password : str
            A senha do usuário para autenticação, obtida do arquivo `.env`.
        
        client_id : str
            O ID do cliente para autenticação, obtido do arquivo `.env`.
        
        url_token : str
            URL para a obtenção do token de autenticação na API.
        
        url_bancos : str
            URL para a obtenção da lista de bancos associados ao cliente autenticado.
        
        url_folha : str
            URL para a obtenção dos registros de pontos dos funcionários.
        
        token : str or None
            Armazena o token de autenticação obtido após o login bem-sucedido. Inicialmente, é definido como None.
        
        Funcionamento:
        --------------
        O método carrega automaticamente as variáveis de ambiente do arquivo `.env` com o `load_dotenv`, preenchendo
        os atributos de autenticação (`grant_type`, `username`, `password`, `client_id`). As URLs específicas para
        autenticação e acesso aos recursos da API também são definidas.
        """


        _ = load_dotenv(find_dotenv())
        # Carregar as variáveis de credenciais da API a partir do .env
        self.grant_type = os.getenv("GRANT_TYPE")
        self.username = os.getenv("API_USERNAME")
        self.password = os.getenv("PASSWORD")
        self.client_id = os.getenv("CLIENT_ID")

        # URLs das autenticações da API
        self.url_token = "https://autenticador.secullum.com.br/Token"
        self.url_bancos = "https://autenticador.secullum.com.br/ContasSecullumExterno/ListarBancos"
        self.url_folha = "https://pontowebintegracaoexterna.secullum.com.br/IntegracaoExterna/Batidas"
        self.token = None

    
    def get_api_token(self):
        """
        Autentica-se na API e obtém um token de acesso para uso nas requisições subsequentes.

        Parâmetros:
        ----------
        Nenhum.

        Retorno:
        -------
        str or None
            Retorna o token de autenticação (access token) como uma string se a requisição for bem-sucedida (status 200).
            Em caso de falha na autenticação, retorna None.

        Exemplo de Uso:
        --------------
        # Exemplo de uso da função para obter o token de acesso
        token = self.get_api_token()

        # A variável `token` conterá o token de autenticação se a requisição for bem-sucedida.
        """
        
        data = {
            "grant_type": self.grant_type,
            "username": self.username,
            "password": self.password,
            "client_id": self.client_id
        }

        # Fazendo a requisição para obter o token
        response = requests.post(self.url_token, data=data)

        # Checando se a autenticação foi bem-sucedida
        if response.status_code == 200:
            self.token = response.json().get("access_token")
        else:
            print("Erro de autenticação:", response.status_code, response.text)
        
        return self.token

    def get_api_bancos(self, token: str):
        """
        Obtém a lista de bancos disponíveis e armazena o ID do primeiro banco na instância da classe.

        Parâmetros:
        ----------
        token : str
            O token de autenticação necessário para acessar a API.

        Retorno:
        -------
        str or None
            Retorna o ID do primeiro banco na lista de bancos se a requisição for bem-sucedida (status 200).
            Em caso de falha, retorna None.

        Exemplo de Uso:
        --------------
        # Exemplo de uso da função para obter o ID do banco
        id_banco = self.get_api_bancos(token="seu_token")

        # A variável `id_banco` conterá o ID do primeiro banco obtido, se a requisição for bem-sucedida.
        """
        
        headers = {
            "Authorization": f"Bearer {token}"
        }

        response = requests.get(self.url_bancos, headers=headers)

        if response.status_code == 200:
            bancos = response.json()
            self.id_banco = bancos[0].get('id')
        else:
            print("Erro ao listar bancos:", response.status_code, response.text)
        
        return self.id_banco


class DataAPI(PontoWebAPI):

    def __init__(self):
        """
        Inicializa uma instância da classe que herda de (PontoWebAPI) uma superclasse e define a URL base para a API de funcionários.

        Atributos:
        ----------
        url_funcionarios : str
            A URL da API usada para obter os dados dos funcionários.

        Funcionamento:
        --------------
        Este método chama o construtor da superclasse usando `super().__init__()` para garantir que quaisquer
        atributos e configurações da superclasse sejam inicializados primeiro. Em seguida, ele define `self.url_funcionarios`
        com a URL específica para chamadas relacionadas aos funcionários na API.
        """
        super().__init__()
        self.url_funcionarios = "https://pontowebintegracaoexterna.secullum.com.br/IntegracaoExterna/Funcionarios"


    def get_funcionarios(self, banco_id):
        """
        Obtém a lista de funcionários de um banco específico, autenticando com um token previamente gerado.

        Parâmetros:
        ----------
        banco_id : str
            O identificador do banco de dados para o qual a lista de funcionários será buscada.

        Retorno:
        -------
        dict or None
            Retorna um dicionário com os dados dos funcionários se a requisição for bem-sucedida (status 200).
            Em caso de falha ou se o token estiver vazio, retorna None.

        Exemplo de Uso:
        --------------
        # Exemplo de uso da função para obter funcionários
        funcionarios = self.get_funcionarios(banco_id="1")

        # A variável `funcionarios` conterá os dados dos funcionários do banco especificado, caso a requisição seja bem-sucedida.
        """
        
        if not self.token:
            print("Erro: Autenticação necessária. Token Vazio.")
            return None

        headers = {
            "Authorization": f"Bearer {self.token}",
            "secullumidbancoselecionado": f"{banco_id}"
        }

        # Fazendo a requisição para obter funcionários
        response = requests.get(self.url_funcionarios, headers=headers)

        if response.status_code == 200:
            dfuncionarios = response.json()
            print("dfuncionarios")
            return dfuncionarios  # json_normalize(dfuncionarios) (ativado para formatar como DataFrame)
        else:
            print("Erro ao obter funcionários:", response.status_code)
            return None

    
    def get_pontos(self, token: str, id_banco: str, data_inicio: str, data_fim: str) -> json_normalize:
        """
        Obtém os dados de pontos de funcionários para um determinado intervalo de datas, usando autenticação por token.

        Parâmetros:
        ----------
        token : str
            O token de autenticação para acessar a API.
        
        id_banco : str
            O identificador do banco de dados que será acessado.
        
        data_inicio : str
            A data inicial do intervalo para buscar os dados de pontos, no formato 'YYYY-MM-DD'.
        
        data_fim : str
            A data final do intervalo para buscar os dados de pontos, no formato 'YYYY-MM-DD'.

        Retorno:
        -------
        json_normalize
            Retorna os dados dos pontos em formato JSON (ou uma versão normalizada para DataFrame, se ativado).
        
        Exemplo de Uso:
        --------------
        # Exemplo de uso da função
        folha_de_ponto = self.get_pontos(token="seu_token", id_banco="1", data_inicio="2024-01-01", data_fim="2024-01-31")
        
        # A variável `folha_de_ponto` conterá os dados de ponto dos funcionários para o período especificado.
        """
        
        headers = {
            "Authorization": f"Bearer {token}",
            "secullumidbancoselecionado": f"{id_banco}"
        }

        params = {
            "dataInicio": data_inicio,
            "dataFim": data_fim,
        }
        response = requests.get(self.url_folha, headers=headers, params=params)

        # Checando a resposta
        if response.status_code == 200:
            folha_de_ponto = response.json()
            print("Folha de ponto: ")
        else:
            print("Erro:", response.status_code, response.text)

        return folha_de_ponto  # json_normalize(folha_de_ponto) (ativado para formatar como DataFrame)

    
    def etl_api(self, data: dict, funcao):
        """
        Executa uma transformação ETL em um conjunto de dados fornecido, aplicando uma função de transformação
        a cada registro e retornando os resultados em um DataFrame do pandas. Caso seja necessário puxa alguma nova
        coluna, ir no arquivo "columns_json.py" e adicionar a nova coluna.

        Parâmetros:
        ----------
        data : dict
            Um dicionário contendo os dados que serão processados. Cada item no dicionário representa um registro a ser transformado.
        
        funcao : function
            Uma função de transformação que será aplicada a cada registro no conjunto de dados.
            A função deve aceitar um registro (dado individual) como entrada e retornar um dicionário ou objeto transformado.
        
        Retorno:
        -------
        pandas.DataFrame
            Retorna um DataFrame do pandas contendo todos os registros transformados, com cada registro armazenado como uma linha.
            Os dados são convertidos para o tipo string (`dtype=str`), garantindo consistência de dados na tabela final.
        
        Exemplo de Uso:
        --------------
        # Definindo uma função de transformação para um registro de exemplo
        def transforma_registro(record):
            return {
                "id": record.get("id"),
                "nome": record.get("nome").upper(),
                "idade": record.get("idade")
            }

        # Exemplo de dados de entrada
        dados = [{"id": 1, "nome": "João", "idade": 30}, {"id": 2, "nome": "Maria", "idade": 25}]

        # Utilizando a função etl_api para transformar os dados
        df = etl_api(dados, transforma_registro)
        
        # O DataFrame `df` conterá os dados transformados:
        #     id   nome   idade
        # 0    1   JOÃO     30
        # 1    2   MARIA    25
        
        """
        
        rows = []
        for record in data:
            row = funcao(record)
            rows.append(row)
        return pd.DataFrame(rows, dtype=str)

    
    def etl_dataframe(self, dataframe: pd.DataFrame, plus: bool = True):
        """
        Realiza uma transformação ETL em um DataFrame do pandas, ajustando os nomes das colunas e, 
        opcionalmente, formatando a coluna de data de nascimento. Podendo ser adicionado novas linhas
        de formatação do dataframe.

        Parâmetros:
        ----------
        dataframe : pandas.DataFrame
            O DataFrame que será processado. Espera-se que o DataFrame contenha uma coluna chamada 'Nascimento'
            se o parâmetro `plus` estiver definido como True.

        plus : bool, opcional
            Um parâmetro opcional (padrão é True) que determina se a coluna 'Nascimento' será convertida 
            para o formato de data padrão ('%Y-%m-%d'). Se `plus` for False, o DataFrame será retornado sem 
            qualquer transformação na coluna 'Nascimento'.
        
        Retorno:
        -------
        pandas.DataFrame
            Retorna o DataFrame com as transformações aplicadas:
            - Todos os pontos ('.') nos nomes das colunas são substituídos por underscores ('_').
            - A coluna 'Nascimento' é convertida para o formato de data '%Y-%m-%d', caso `plus` seja True.
        
        Exemplo de Uso:
        --------------
        # Exemplo de DataFrame de entrada
        data = {
            "Nome.Funcionario": ["Ana", "João"],
            "Nascimento": ["1990-05-15", "1985-08-20"]
        }
        df = pd.DataFrame(data)
        
        # Instância da classe e chamada da função
        etl_instance = ClasseExemplo()
        df_tratado = etl_instance.etl_dataframe(df, plus=True)
        
        # O DataFrame `df_tratado` conterá as seguintes alterações:
        #    Nome_Funcionario   Nascimento
        # 0             Ana     1990-05-15
        # 1            João     1985-08-20

        """
        dataframe.columns = dataframe.columns.str.replace('.', '_')

        if plus:  # Tratamento para converter a data da tabela de funcionários
            dataframe['Nascimento'] = pd.to_datetime(dataframe['Nascimento'], errors='coerce').dt.strftime('%Y-%m-%d')

        return dataframe


import os
import pyodbc
from time import sleep
from datetime import timedelta
from sqlalchemy import create_engine

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

    def parametro_data(self):
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
                    print('Conexão realizada para obter os intervalos de data.')

                    with conexao.cursor() as cursor:
                        cursor.execute(PEGAR_INTERVALO)
                        result = cursor.fetchone()
                        data_inicial = result.DataInicial
                        data_final = result.DataFinal
                
                data_inicial_ajustada = data_inicial + timedelta(days=1)
                print(f'Intervalo: {data_inicial} --> {data_final}')
                print(f'Ajustado: {data_inicial_ajustada}')
                
                return data_inicial.strftime('%Y-%m-%d'), data_final.strftime('%Y-%m-%d')

            except pyodbc.Error as e:
                tentativa_atual += 1
                print(f"Erro na conexão (tentativa {tentativa_atual} de {tentativas}):", e)
                if tentativa_atual < tentativas:
                    print("Tentando novamente em 5 segundos...")
                    sleep(5)  # Aguarda antes de tentar novamente

        print("Não foi possível estabelecer conexão após várias tentativas.")
        return None, None

    
    def carregar_dados_db(self, dataframe, table_name, funcionario=False):
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
                        print(f"Conectado ao banco de dados: {db_name}")
                        
                        if funcionario:
                            print("Executando função com funcionários")
                            cursor.execute(DEL_TABELA_FUNCIONARIO)
                        else:
                            print("Executando função SEM funcionários")
                            cursor.execute(DELETAR_INTERVALO)
                        conexao.commit()
                        print("Intervalo de dados deletado com sucesso.")

                engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(self.string_connection))
                dataframe.to_sql(table_name, engine, if_exists='append', index=False)
                print(f"Dados inseridos na tabela {table_name} com sucesso.")
                return  # Quebra o loop quando a tentativa for de conexão com o banco for ok

            except Exception as e:
                tentativa_atual += 1
                print(f"Erro ao carregar dados no banco de dados (tentativa {tentativa_atual} de {tentativas}):", e)

                # Se a conexão cair, volta o estado anterior.
                if 'conexao' in locals():
                    conexao.rollback()

                if tentativa_atual < tentativas:
                    print("Tentando novamente em 5 segundos...")
                    sleep(5)

            finally:
                if engine:
                    engine.dispose()

        print("Não foi possível carregar os dados após várias tentativas.")



if __name__ == '__main__':

    ## Chamamdo minhas classes
    conexao = DatabaseSQL()
    dados = DataAPI()
    ## -------------------------

    token = dados.get_api_token()
    print('peguei token')
    banco_id = dados.get_api_bancos(token=token)
    print('pegando parametro data')
    data_inicial_ajustada, data_fim = conexao.parametro_data()
    
    data_fim = datetime.now().strftime('%Y-%m-%d')
    df_funcionarios = dados.get_funcionarios(banco_id=banco_id)

    # data_inicio = "2024-09-29"
    # data_fim = "2024-11-07"

    df_pontos = dados.get_pontos(token=token, id_banco=banco_id, data_inicio=data_inicial_ajustada, data_fim=data_fim)


    raw_df1 = dados.etl_api(data=df_funcionarios, funcao=colunas_dfuncionarios)
    raw_df2 = dados.etl_api(data=df_pontos, funcao=colunas_folhaPonto)

    final_df1 = dados.etl_dataframe(raw_df1)
    final_df2 = dados.etl_dataframe(raw_df2, plus=False)

    alimentar_tabela_funcionario = conexao.carregar_dados_db(final_df1, table_name=TABELA_FUNCIONARIO, funcionario=True)
    alimentar_tabela_ponto = conexao.carregar_dados_db(dataframe=final_df2, table_name=TABELA_PONTO)
    
