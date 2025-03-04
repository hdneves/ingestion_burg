from dotenv import load_dotenv, find_dotenv

import requests
import os
from logging import Logger


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
        #self.logger = logging.getLogger(__name__)

    
    def get_api_token(self, logger:Logger):
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
            logger.error(f"Erro de autenticação: {response.status_code} {response.text}")
        logger.info('Pegou token')
        return self.token

    def get_api_bancos(self, token: str, logger:Logger):
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
            id_banco = bancos[0].get('id')
        else:
            logger.error(f"Erro ao listar bancos: {response.status_code}, {response.text}")
        
        logger.info('Pegou numero banco')
        return id_banco
        #self.id_banco
