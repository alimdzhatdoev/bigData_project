# Базовый образ Python 3.11
FROM python:3.11

# Установить Java для PySpark
RUN apt-get update && apt-get install -y default-jdk

# Установить зависимости проекта
COPY requirements.txt .
RUN pip install -r requirements.txt

# Установить переменные окружения для Java
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH"

# Скопировать проект
COPY . .

# Запустить Streamlit
CMD ["streamlit", "run", "main.py", "--server.port=8000", "--server.enableCORS=false"]
