# Вводные
Есть CSV-файл (https://disk.yandex.ru/d/npzjVSfFyH7Cww) с данными по рекомендациям одних товаров к другим, по одной рекомендации на строку:

sku_1,sku_recom_1,0.1
sku_2,sku_recom_2,0.2
…

Последний столбец (rank) содержит вероятность правильной рекомендации, то есть “близость” рекомендованного товара к исходному (чем больше значение — тем больше “близость”).

# Задание
Написать Web API на Python с одним эндпоинтом, который принимает sku в качестве параметра и возвращает список всех товаров, рекомендованных для этого sku. Должна быть возможность указать опциональный параметр минимального порога близости для рекомендаций.
Подготовить проект для деплоя (привести в порядок структуру проекта, проверить актуальность файла с зависимостями, создать README с примером запуска и т.д.)

# Ограничения на решение
1. Задача должна быть решена без использования сторонних библиотек и сервисов (вроде баз данных). Допустимо использование только стандартной библиотеки Python. Исключение: можно воспользоваться Flask или AIOHTTP для реализации Web API, но реализация только на стандартной библиотеке — плюс.
2. Модуль sqlite3 использовать нельзя.
3. CSV-файл можно предварительно подготовить другим инструментом, если нужно.
4. Максимальное время ответа API на запрос — 500 мс.
5. Максимальное потребление оперативной памяти сервером — 5 Гб.
6. При выборе между меньшей скоростью ответа на запрос и меньшим потреблением оперативной памяти предпочтение отдавать меньшей скорости ответа.