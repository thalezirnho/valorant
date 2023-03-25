FROM python:3.9-slim-buster

# define o diretório de trabalho
RUN mkdir /datalake
WORKDIR /datalake

# copia o arquivo requirements.txt para o contêiner
COPY requirements.txt .

# instala as dependências do projeto
RUN pip install -r requirements.txt

# copia o restante do código para o contêiner
COPY . .

# define o comando padrão que será executado quando o contêiner for iniciado
CMD [ "python"]