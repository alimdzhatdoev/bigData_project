# Используем базовый образ Python
FROM python:3.11

# Установить Java для PySpark
RUN apt-get update && apt-get install -y default-jdk

# Установить рабочую директорию
WORKDIR /app

# Скопировать файл зависимостей
COPY requirements.txt .

# Установить Python-зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Установить переменные окружения для Java
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH"

# Скопировать все файлы проекта
COPY . .

# Указать порт для Railway
EXPOSE 8000

# Запустить Streamlit
CMD ["streamlit", "run", "main.py", "--server.port=8000", "--server.enableCORS=false"]
