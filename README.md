# Проект 7-го спринта

### Описание
Репозиторий предназначен для сдачи проекта 7-го спринта

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-sprint-7` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой компьютер. В качестве пароля укажите ваш `Access Token`, который нужно получить на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/{{ username }}/de-project-sprint-7.git`
3. Перейдите в директорию с проектом: 
	* `cd de-project-sprint-7`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GutHub-аккаунте:
	* `git push origin main`

### Структура репозитория
Вложенные файлы в репозиторий будут использоваться для проверки и предоставления обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре — так будет проще соотнести задания с решениями.

Внутри `src` расположены две папки:
- `/src/dags`
	- de-sprint-7-project-dag-initial-load.py — Инициальная загрузка данных выполняется в ручную (однократно)
	- de-sprint-7-project-dag1.py — DAG обновляет слой STG из источника и производит расчет метрик витрины 1 и 3 (на заданную глубину)
 	- de-sprint-7-project-dag2.py — производит расчет метрик витрины 2 (на заданную глубину)
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
 
