FROM apache/airflow:latest
RUN pip install --no-cache-dir soda-core-postgres pandas requests sqlalchemy psycopg2-binary google-genai python-telegram-bot