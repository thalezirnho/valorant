# valorant
 
Api: https://docs.henrikdev.xyz/valorant.html


## Open AI GPT-3
Configuração do ambiente de desenvolvimento:
a. Crie uma instância do Compute Engine com uma configuração adequada de CPU e memória, como a e2-micro.

b. Instale o Docker no Compute Engine e crie uma imagem do Docker para a aplicação usando o Dockerfile.

c. Crie um bucket no Cloud Storage para armazenar os arquivos .json coletados.

d. Configure as credenciais do Google Cloud Platform para permitir a autenticação da aplicação no Cloud Storage e BigQuery.

Coletando os dados da API e salvando no Cloud Storage:
a. Escreva um código Python para se conectar à API e coletar os dados de todos os endpoints necessários.

b. Converta as respostas em formato JSON e salve em um arquivo com nome e formato apropriado.

c. Carregue os arquivos .json para o bucket do Cloud Storage usando a biblioteca do Google Cloud Storage para Python.

d. Programe o código Python para ser executado usando o CRON para atualizar os dados com frequência.

Lendo os arquivos .json do Cloud Storage e carregando no BigQuery:
a. Escreva um código Python para ler os arquivos .json do bucket do Cloud Storage e convertê-los em tabelas do BigQuery.

b. Use a biblioteca do Google Cloud Storage para Python e a biblioteca do Google Cloud BigQuery para Python para carregar as tabelas no BigQuery.

c. Verifique se as tabelas foram carregadas corretamente e estejam disponíveis no BigQuery.

Criando a camada Gold no BigQuery:
a. Escreva um código Python para aplicar regras de negócio e gerar tabelas no BigQuery com base na camada Silver.

b. Use a biblioteca do Google Cloud BigQuery para Python para criar as tabelas e aplicar as regras de negócio.

c. Verifique se as tabelas foram criadas corretamente e estejam disponíveis no BigQuery.

Programe a execução de toda a pipeline:
a. Use o CRON para programar a execução de todo o código Python em horários específicos.

b. Monitore o pipeline regularmente para garantir que tudo esteja sendo executado corretamente e sem erros.

Espero que esse guia ajude você a criar e executar sua pipeline de desenvolvimento de engenharia de dados na nuvem com sucesso!