
from pandas import json_normalize
from .ponto_web_api import PontoWebAPI
from logging import Logger
import pandas as pd
import requests


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
        #self.logger = logging.getLogger(__name__)


    def get_funcionarios(self, banco_id, logger:Logger):
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
            logger.info("Erro: Autenticação necessária. Token Vazio.")
            return None

        headers = {
            "Authorization": f"Bearer {self.token}",
            "secullumidbancoselecionado": f"{banco_id}"
        }

        # Fazendo a requisição para obter funcionários
        response = requests.get(self.url_funcionarios, headers=headers)

        if response.status_code == 200:
            dfuncionarios = response.json()
            logger.info("Requisitou o json funcionarios.")
            return dfuncionarios  # json_normalize(dfuncionarios) (ativado para formatar como DataFrame)
        else:
            logger.error(f"Erro ao obter funcionários: {response.status_code}")
            return None

    
    def get_pontos(self, token: str, id_banco: str, data_inicio: str, data_fim: str, logger) -> json_normalize:
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
            logger.info("Requisitou json folha de ponto: ")
        else:
            logger.error(f"Erro: {response.status_code, response.text}")

        return folha_de_ponto  # json_normalize(folha_de_ponto) (ativado para formatar como DataFrame)

    
    def etl_api(self, data: dict, funcao, logger: Logger):
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
        logger.info("Filtrou as colunas necessarias")
        return pd.DataFrame(rows, dtype=str)

    
    def etl_dataframe(self, dataframe: pd.DataFrame,logger: Logger, plus: bool = True):
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
        logger.info("Fez o tratamento no dataframe.")
        return dataframe
