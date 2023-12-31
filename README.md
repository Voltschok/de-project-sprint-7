# Проект 7-го спринта

### Описание
В проекте по работе с Data Lake (de-project-sprint-7) было спроектировано HDFS-хранилище. Витрины данных были спроектированы и рассчитаны с использованием PySpark. Расчет витрин был автоматизирован с помощью SparkSubmitOperator в DAG Airflow.

### Структура репозитория

Внутри `src` расположены две папки:
- `/src/dags`
	- de-sprint-7-project-dag-initial-load.py — Инициальная загрузка данных выполняется в ручную (однократно)
	- de-sprint-7-project-dag1.py — DAG обновляет слой STG из источника и производит расчет метрик витрины 1 и 3 (на заданную глубину ежедневно)
 	- de-sprint-7-project-dag2.py — производит расчет метрик витрины 2 (на заданную глубину еженедельно)
- `/src/scripts`

    - load_data.py — Job инициальной загрузки;
    - update_data.py — Job обновления STG;
    - 1st_vitrin.py — Job расчета метрик пользователя;
    - 2nd_vitrin.py — Job расчета метрик по зонам (городам);
    - 3d_vitrin.py — Job расчета метрик для витрины с рекомендациями друзей.

## Структура хранилища

### Формат данных: Parquet

### Пути к источнику данных:
 - "/user/voltschok/data/geo/events": данные по событиям
 - "/user/voltschok/data/geo/cities": данные по координатам городов и их временным зонам

### Названия директорий:
 - user_address: данные по актуальному и домашнему адресу пользователей
 - city_zone: данные по количеству событий в конкретном городе за неделю и месяц
 - friend_recommendation: данные для рекомендации друзей
   
### Частота обновления данных:
 - user_address: ежедневно
 - city_zone: еженедельно
 - friend_recommendation: ежедневно

### Пути к данным для аналитиков:
 - "/user/voltschok/data/analytics/user_address"
 - "/user/voltschok/data/analytics/friend_recommendation"
 - "/user/voltschok/data/analytics/city_zone"
 
