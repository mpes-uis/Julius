# Define a imagem base a ser usada
FROM python:3.9

# Define o diretório de trabalho
WORKDIR /app

# Clona o repositório
RUN git clone https://github.com/mpes-uis/Julius.git

# Define o diretório de trabalho para o diretório clonado
WORKDIR /app/Julius

# Instala as dependências
RUN pip install -r requirements.txt

# Executa o arquivo main.py
CMD ["python", "main.py"]
