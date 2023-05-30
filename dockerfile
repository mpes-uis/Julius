# Imagem base
FROM continuumio/anaconda3

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia todos os arquivos da pasta App para o diretório de trabalho do container
COPY ./App /app

# Clona o git com a versão mais atual (tirar se der ruim)
RUN git clone https://github.com/mpes-uis/Julius.git

# Copia o arquivo requirements.txt para o diretório de trabalho do container
#COPY requirements.txt .

#Define o diretório para rodar
#local
#WORKDIR /app

#git
WORKDIR /app/Julius

# Instala os pacotes listados no arquivo requirements.txt
#RUN conda install --file requirements.txt --yes

#RUN pip install -r requirements.txt

RUN mkdir output

# Executa o arquivo main.py
CMD ["python", "main.py"]
