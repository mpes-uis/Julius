# Use the official Anaconda base image
FROM continuumio/anaconda3

# Set the working directory inside the container
WORKDIR /app

# Clone the Julius repository from GitHub
RUN git clone https://github.com/mpes-uis/Julius.git

# Define o diretório de trabalho para o diretório clonado
WORKDIR /app/Julius

# Install Python packages from requirements.txt
#RUN conda install --file requirements_old.txt

# Set the entrypoint command to run main.py
CMD ["python", "main.py"]
