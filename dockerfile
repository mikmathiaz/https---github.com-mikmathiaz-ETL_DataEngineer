# Começa com a imagem oficial do Airflow
FROM apache/airflow:latest

# Muda para o usuário 'root' para instalar pacotes do sistema
USER root

# Instala as dependências necessárias para o SQL Server
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc-dev \
    build-essential

# Adiciona a chave e o repositório da Microsoft
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Instala o driver ODBC do SQL Server (msodbcsql17)
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Limpa o lixo da instalação para a imagem ficar leve
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Devolve o controle para o usuário 'airflow'
USER airflow

# Copia o arquivo de requisitos e instala as bibliotecas Python
COPY requirements.txt /
RUN pip install --user -r /requirements.txt