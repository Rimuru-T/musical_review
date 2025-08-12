# Хранилище данных музыкальных рецензий

## О проекте

Проект создает аналитическую базу данных на основе набора данных Amazon Musical Instruments Reviews из Kaggle.

## Стек технологий

- Docker + Docker Compose
- PostgreSQL
- Apache Airflow
- dbt
- Python

## Запуск

1. Скопируйте kaggle.json в папку `secrets/`
2. Создайте файл `.env` с переменными окружения (пароли, fernet key)
3. Запустите контейнеры:

```bash
docker-compose up -d
