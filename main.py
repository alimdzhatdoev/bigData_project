# Для установки всех зависимостей: pip install pandas matplotlib streamlit psutil pyspark
# Запуск: streamlit run main.py
# sudo apt update
# sudo apt install default-jdk

import os

# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
# os.environ["PATH"] = "/usr/lib/jvm/java-11-openjdk-amd64/bin:" + os.environ["PATH"]

import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import streamlit as st
import time
import psutil
from concurrent.futures import ThreadPoolExecutor

# Создаем SparkSession один раз
@st.cache_resource
def get_spark_session():
    return SparkSession.builder \
        .appName("Анализ данных по COVID-19") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

# Инициализация Spark
spark = get_spark_session()

# Очистка названий колонок
def clean_column_name(col):
    return col.strip().replace(" ", "_").replace("/", "_").replace("-", "_").replace(".", "_")

def clean_pandas_columns(df):
    """Очищает названия колонок в Pandas DataFrame"""
    df.columns = [col.strip().replace(" ", "_").replace("(", "").replace(")", "") for col in df.columns]
    return df

# Переименование колонок на русский
def rename_columns_to_russian(df):
    rename_map = {
        "Province_State": "Провинция_Штат",
        "Country_Region": "Страна_Регион",
        "Lat": "Широта",
        "Long": "Долгота",
        "Date": "Дата",
        "Confirmed": "Подтвержденные",
        "Deaths": "Смерти",
        "Recovered": "Выздоровевшие",
        "Active": "Активные",
        "WHO_Region": "Регион_ВОЗ",
    }
    df.rename(columns=rename_map, inplace=True)
    return df

# Преобразование в Pandas DataFrame
def spark_to_pandas(spark_df):
    pandas_df = spark_df.toPandas()
    return clean_pandas_columns(pandas_df)

# Загрузка и обработка данных
@st.cache_data
def load_and_process_data(file_path):
    start_time = time.time()
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    clean_columns = [clean_column_name(col) for col in df.columns]
    df = df.toDF(*clean_columns)
    pandas_df = spark_to_pandas(df)
    pandas_df = rename_columns_to_russian(pandas_df)
    processing_time = time.time() - start_time
    return pandas_df, processing_time

# Получение размера файла
def get_file_size(file_path):
    size_in_bytes = os.path.getsize(file_path)
    size_in_mb = size_in_bytes / (1024 * 1024)
    return size_in_mb

# Нагрузочное тестирование
def simulate_load(test_function, iterations):
    results = []
    with ThreadPoolExecutor() as executor:
        for result in executor.map(test_function, range(iterations)):
            results.append(result)
    return results

def test_function(iteration):
    """Функция для имитации нагрузки."""
    start_time = time.time()
    dummy_df = spark.read.csv("data/covid_19_clean_complete.csv", header=True, inferSchema=True)
    dummy_df.count()
    end_time = time.time()
    return {
        "Итерация": iteration + 1,
        "Время_начала": start_time,
        "Время_окончания": end_time,
        "Время_выполнения": end_time - start_time,
    }

# Основное приложение
if __name__ == "__main__":
    st.title("Анализ данных по COVID-19")

    # Загрузка файла covid_19_clean_complete.csv
    file_path = "data/covid_19_clean_complete.csv"

    # Мониторинг системных ресурсов
    process = psutil.Process()
    start_cpu = process.cpu_percent(interval=None)
    start_memory = process.memory_info().rss / (1024 * 1024)

    pandas_df, processing_time = load_and_process_data(file_path)

    end_cpu = process.cpu_percent(interval=None)
    end_memory = process.memory_info().rss / (1024 * 1024)

    file_size = get_file_size(file_path)
    num_rows, num_columns = pandas_df.shape

    # Создание вкладок
    tab1, tab2 = st.tabs(["Приложение", "Производительность"])

    with tab1:
        # Отображение исходных данных
        st.header("Исходные данные")
        st.dataframe(pandas_df, use_container_width=True)
        st.write("Общее количество записей: ", len(pandas_df))

        # Объединение данных по странам
        st.header("Данные, объединенные по странам")
        aggregated_df = pandas_df.groupby("Страна_Регион", as_index=False).agg({
            "Подтвержденные": "sum",
            "Смерти": "sum",
            "Выздоровевшие": "sum",
            "Активные": "sum",
        })
        st.dataframe(aggregated_df, use_container_width=True)

        # График по странам
        st.header("График по метрикам")
        numeric_columns = ["Подтвержденные", "Смерти", "Выздоровевшие", "Активные"]
        default_columns = ["Подтвержденные"]

        num_countries = st.slider("Количество стран для отображения", min_value=5, max_value=len(aggregated_df), value=10, step=1)
        country_options = aggregated_df["Страна_Регион"].dropna().unique().tolist()
        specific_country = st.selectbox("Выберите страну для отображения (опционально)", options=[""] + sorted(country_options))

        selected_columns = st.multiselect(
            "Выберите поля для отображения на графике", options=numeric_columns, default=default_columns
        )

        if selected_columns:
            if specific_country:
                specific_data = aggregated_df[aggregated_df["Страна_Регион"] == specific_country]
                if not specific_data.empty:
                    st.bar_chart(specific_data.set_index("Страна_Регион")[selected_columns])
                    st.write(f"Данные для выбранной страны: {specific_country}")
                else:
                    st.write("Страна не найдена в данных.")
            else:
                top_countries_data = aggregated_df.nlargest(num_countries, selected_columns[0])
                st.bar_chart(top_countries_data.set_index("Страна_Регион")[selected_columns])
        else:
            st.write("Пожалуйста, выберите хотя бы одну метрику для отображения.")

        # Объединение данных по датам
        st.header("Данные, объединенные по датам")
        date_aggregated_df = pandas_df.groupby("Дата", as_index=False).agg({
            "Подтвержденные": "sum",
            "Смерти": "sum",
            "Выздоровевшие": "sum",
            "Активные": "sum",
        })
        st.dataframe(date_aggregated_df, use_container_width=True)

        # График по датам
        st.header("График по датам")
        selected_date_columns = st.multiselect(
            "Выберите поля для отображения на графике по датам", options=numeric_columns, default=default_columns, key="date_columns"
        )

        if selected_date_columns:
            st.line_chart(date_aggregated_df.set_index("Дата")[selected_date_columns])
        else:
            st.write("Пожалуйста, выберите хотя бы одну метрику для отображения на графике по датам.")

    with tab2:
        # Отображение производительности
        st.header("Производительность")
        st.subheader("Результаты анализа производительности:")
        st.write(f"Размер файла: {file_size:.2f} MB")
        st.write(f"Количество строк: {num_rows}")
        st.write(f"Количество столбцов: {num_columns}")
        st.write(f"Время загрузки и обработки данных: {processing_time:.2f} секунд")
        st.write(f"Использование CPU: {end_cpu - start_cpu:.2f}%")
        st.write(f"Использование памяти: {end_memory - start_memory:.2f} MB")

        # Нагрузочное тестирование
        st.subheader("Нагрузочное тестирование")
        iterations = st.slider("Количество итераций для нагрузки", min_value=1, max_value=100, value=10, step=1)
        if st.button("Начать нагрузочное тестирование"):
            load_results = simulate_load(test_function, iterations)

            # Таблица с результатами всех итераций
            st.write("Результаты по каждой итерации:")
            results_df = pd.DataFrame(load_results)
            results_df["Время_начала"] = pd.to_datetime(results_df["Время_начала"], unit="s")
            results_df["Время_окончания"] = pd.to_datetime(results_df["Время_окончания"], unit="s")
            st.dataframe(results_df)

            # Среднее, минимальное и максимальное время
            avg_time = results_df["Время_выполнения"].mean()
            min_time = results_df["Время_выполнения"].min()
            max_time = results_df["Время_выполнения"].max()
            total_time = results_df["Время_выполнения"].sum()

            st.write(f"Общее время выполнения всех итераций: {total_time:.2f} секунд")
            st.write(f"Среднее время выполнения одной итерации: {avg_time:.2f} секунд")
            st.write(f"Минимальное время выполнения: {min_time:.2f} секунд")
            st.write(f"Максимальное время выполнения: {max_time:.2f} секунд")

            # График временных затрат на итерации
            st.line_chart(results_df["Время_выполнения"])

            # Выводы по результатам тестирования
            st.subheader("Выводы")
            if avg_time < 1:
                st.write(
                    "Приложение демонстрирует **высокую производительность** с минимальным временем выполнения одной итерации.")
            elif 1 <= avg_time <= 3:
                st.write(
                    "Приложение работает с **умеренной производительностью**, время выполнения находится в приемлемых пределах.")
            else:
                st.write(
                    "Приложение демонстрирует **низкую производительность**, возможны проблемы в алгоритмах или перегрузка системы.")

            if max_time > avg_time * 1.5:
                st.write(
                    "**Замечено значительное отклонение в максимальном времени выполнения**, что может указывать на нестабильность.")
            else:
                st.write("Максимальное время выполнения близко к среднему, что свидетельствует о стабильной работе.")

            if end_cpu - start_cpu < 50:
                st.write("**Процессор используется оптимально**, без перегрузок.")
            elif 50 <= end_cpu - start_cpu <= 80:
                st.write("**Высокая загрузка процессора**, рекомендуется следить за нагрузкой.")
            else:
                st.write("**Процессор перегружен**, возможны риски ухудшения производительности.")

            if end_memory - start_memory > 500:
                st.write(
                    "**Обнаружено значительное увеличение использования памяти**, что может указывать на утечку памяти или высокую нагрузку.")
            else:
                st.write("Использование памяти находится в пределах нормы.")
