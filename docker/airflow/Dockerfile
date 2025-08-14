FROM apache/airflow:2.9.2-python3.12
        
USER root
# Устанавливаем Java для PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    curl \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем JAVA_HOME 
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin

# Настройки pip для стабильных загрузок
ENV PIP_DEFAULT_TIMEOUT=1200 \
    PIP_RETRIES=10 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Проверяем, что Java установлена
RUN java -version

USER airflow

# Копируем requirements и устанавливаем зависимости
COPY airflow-requirements.txt /requirements.txt
RUN pip install --no-cache-dir --timeout 1200 -r /requirements.txt

# Устанавливаем PySpark через pip
RUN pip install --no-cache-dir --timeout 1200 pyspark==3.4.0

WORKDIR /opt/airflow