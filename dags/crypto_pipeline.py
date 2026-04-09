from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python import BranchPythonOperator

from soda.scan import Scan

from datetime import datetime
from dotenv import load_dotenv
from google import genai
import pandas as pd
import sys
import os
import sqlalchemy
import json
import asyncio
from telegram import Bot

sys.path.append(os.path.join(os.path.dirname(__file__)))
from fetch_crypto import fetch_crypto_data, save_to_database, fetch_crypto_news, save_news_to_database

dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)

default_args = {
    "owner": "Mathieu",
    "depends_on_past": False,
    "start_date" : datetime(2026, 4, 7),
    "retries": 0,
}

dag =  DAG(
    "dag_crypto_soda",
    default_args=default_args,
    description="Pipeline de collecte crypto avec contrôle Soda",
    schedule=None, # Cron, se lance toutes les 30 minutes "*/30 * * * *"
    catchup=False,
    tags=['demonstration', 'soda', 'crypto']
)

def fetch_task_wrapper():
    """Récupère les données de l'API CoinGecko et les renvoie pour la tâche suivante"""
    data = fetch_crypto_data()
    return data

def fetch_news_wrapper():
    """Récupère les données de l'API NewsData et les renvoie pour la tâche suivante"""
    data = fetch_crypto_news()
    return data

def fetch_task_error_price():
    """Récupère les données de l'API mais modifie des données pour provoquer des erreurs"""

    data = fetch_crypto_data()
    data.loc[data["crypto_id"] == "solana", "price_euro"] = -100
    return data

def save_task_wrapper(ti):
    """Récupère les données de la tâche précédente et les sauvegarde dans la DB"""

    data_extracted = ti.xcom_pull(task_ids="fetch_data")

    if data_extracted is not None and not data_extracted.empty:
        save_to_database(data_extracted)
    else:
        raise ValueError("Aucune donnée reçue de fetch")
    
def save_task_news_wrapper(ti):
    """Récupère les données de la tâche précédente et les sauvegarde dans la DB"""

    data_extracted = ti.xcom_pull(task_ids="fetch_news")
    if data_extracted is not None and not data_extracted.empty:
        save_news_to_database(data_extracted)
    else:
        raise ValueError("Aucune donnée reçue de fetch")
    
def run_soda_scan(check_file_name, ti=None):
    scan = Scan()
    
    scan.set_data_source_name("my_postgres_db") 

    scan.add_configuration_yaml_file(file_path="/opt/airflow/configuration.yml")
    scan.add_sodacl_yaml_files(f"/opt/airflow/{check_file_name}")

    exit_code = scan.execute()

    if scan.has_error_logs():
        raise ValueError(f"Soda a rencontré une erreur critique avec {check_file_name}. Regarde les logs.")

    results = scan.get_checks_fail()
    failed_check_names = [check.name for check in results]
    print(f"Test échoués détectés dans {check_file_name} : {failed_check_names}")


    if check_file_name != "checks_crypto.yml" and len(failed_check_names) > 0:
         raise ValueError(f"Les tests de qualité ont échoué pour {check_file_name} : {failed_check_names}")

    return failed_check_names

def logic_fix_data():
    engine = sqlalchemy.create_engine("postgresql://admin:admin@postgres:5432/airflow_db")
    query = "DELETE FROM crypto_prices WHERE price_euro <= 0"
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(query))
        conn.commit()
    print("Correction terminée : les prix négatifs ont été supprimés.")

def error_analysis(ti):
    failed_checks = ti.xcom_pull(task_ids='soda_quality_check')
    
    if not failed_checks:
        return "clean_pipeline_done"
        
    if any("positif" in name.lower() for name in failed_checks):
        return "fix_negative_price"
    
    if any("duplicate" in name.lower() for name in failed_checks):
        return "fix_doublons"
        
    return "clean_pipeline_done"

def ai_analysis():
    # Connexion à la DB
    engine = sqlalchemy.create_engine("postgresql://admin:admin@postgres:5432/airflow_db")

    # Historique des prix
    query_prices = """
        SELECT timestamp, price_euro
        FROM crypto_prices
        WHERE crypto_id = 'bitcoin'
        ORDER BY timestamp DESC
        LIMIT 30
    """
    history_prices = pd.read_sql(query_prices, engine)

    # Historique des news
    query_news = """
        SELECT pub_date, title, description
        FROM crypto_news
        ORDER BY pub_date DESC
        LIMIT 5
    """
    history_news = pd.read_sql(query_news, engine)

    prompt = f"""
        Tu es un analyste financier expert en cryptomonnaies.

        Voici l'historique des prix récents du Bitcoin : 
        {history_prices.to_json(orient="records")}

        Voici les actualités récentes concernant le Bitcoin : 
        {history_news.to_json(orient="records")}

        Croise l'évolution des prix avec le contexte des actualités pour fournir ton analyse. 
        Réponds STRICTEMENT sous ce format :
        MOUVEMENT: [Hausse/Baisse/Stable]
        SENTIMENT_NEWS: [Positif/Négatif/Neutre]
        RAISON: [Ton explication claire et concise]
    """

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("La variable d'environnement GEMINI_API_KEY n'est pas définie.")

    client = genai.Client(api_key=api_key)
    
    max_retry = 3

    for i in range(1, max_retry + 1):

        print(f"--- Lancement de Gemini (Essai n°{i}/{max_retry}) ---\n")

        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash-lite", #gemini-2.5-flash-lite / gemini-3.1-flash-lite-preview
                contents=prompt
            )

            texte_rapport = response.text
            print("Analyse générée avec succès :")
            print(texte_rapport)

            try:
                with engine.connect() as conn:
                    conn.execute(sqlalchemy.text("DELETE FROM ia_reports;"))
                    conn.commit()
            except Exception:
                pass

            df_report = pd.DataFrame({
                "date_report": [pd.Timestamp.now()],
                "content": [texte_rapport]
            })
            
            df_report.to_sql('ia_reports', engine, if_exists="append", index=False)

            print("Vérification du format par scan Soda :\n")
            scan = Scan()
            scan.set_data_source_name("my_postgres_db")
            scan.add_configuration_yaml_file(file_path="/opt/airflow/configuration.yml")
            scan.add_sodacl_yaml_files("/opt/airflow/checks_ai.yml")
            scan.execute()

            if scan.has_error_logs():
                raise ValueError("Soda a planté car le fichier checks_ai.yml a une erreur de syntaxe ou la table ia_reports n'existe pas.")

            format_errors = scan.get_checks_fail()

            if len(format_errors) == 0:
                print("Format valide. Gemini a produit le bon résultat\n")
                return texte_rapport
            else:
                errors_name = [c.name for c in format_errors]
                print(f"Format invalide détecté par Soda : {errors_name}")
                errors_list_text = "\n- ".join(errors_name)

                prompt = f"""
                    Tu as généré ce texte précédemment :
                    "{texte_rapport}"

                    Cependant, un système de contrôle de qualité automatique a rejeté ton texte pour les raisons suivantes :
                    - {errors_list_text}

                    Génère une nouvelle analyse en corrigeant IMPÉRATIVEMENT ces erreurs de format.
                    """
        except Exception as e:
            print(f"Erreur lors de l'appel à l'IA : {e}")   
            raise e
        
    raise ValueError(f"Gemini a échoué à produire le bon format après {max_retry} tentatives.")


def check_if_12h():
    now = datetime.now()
    # return now.hour == 12 and now.minute < 3
    return True

async def send_telegram(message):
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("CHAT_ID")

    if not token or not chat_id:
        raise ValueError("Variables Telegram manquantes")
    
    bot = Bot(token=token)

    await bot.send_message(
        chat_id=chat_id,
        text=message
        )
    
def send_telegram_alert(ti):
    ai_report = ti.xcom_pull(task_ids="gemini_analysis")

    if not ai_report:
        raise ValueError("Aucun rapport n'a été produit par Gemini")
    

    asyncio.run(send_telegram(ai_report))
    print("Message envoyé avec succès sur Telegram !")


task_fetch_crypto = PythonOperator(
    task_id = "fetch_data",
    python_callable = fetch_task_error_price,
    dag = dag
)

task_save_crypto = PythonOperator(
    task_id = "save_to_db",
    python_callable = save_task_wrapper,
    dag = dag
)

task_fetch_news = PythonOperator(
    task_id = "fetch_news",
    python_callable = fetch_news_wrapper,
    dag = dag
)

task_save_news = PythonOperator(
    task_id = "save_news_to_db",
    python_callable = save_task_news_wrapper,
    dag = dag
)

task_soda_crypto = PythonOperator(
    task_id = "soda_quality_check",
    python_callable = run_soda_scan,
    op_args = ["checks_crypto.yml"],
    dag = dag
)

task_soda_news = PythonOperator(
    task_id = "soda_check_news",
    python_callable = run_soda_scan,
    op_args = ["checks_news.yml"],
    dag = dag
)

# task_soda_ia = PythonOperator(
#     task_id = "soda_check_ai",
#     python_callable = run_soda_scan,
#     op_args = ["checks_ai.yml"],
#     dag = dag
# )

task_branching = BranchPythonOperator(
    task_id = "choose_fix",
    python_callable = error_analysis,
    dag = dag
)

task_fix_doublons = PythonOperator(
    task_id = "fix_doublons",
    python_callable = lambda: print("Suppression des doublons en SQL"),
    dag = dag
)

task_fix_price = PythonOperator(
    task_id = "fix_negative_price",
    python_callable = logic_fix_data,
    dag = dag
)

task_final_ok = BashOperator(
    task_id = "clean_pipeline_done",
    bash_command = 'echo "Les données en base sont maintenant propres"',
    trigger_rule = "none_failed",
    dag = dag
)

task_gemini = PythonOperator(
    task_id = "gemini_analysis",
    python_callable = ai_analysis,
    dag = dag
)

task_is_it_time = ShortCircuitOperator(
    task_id = "condition_midi",
    python_callable=  check_if_12h,
    dag=dag
)

task_telegram = PythonOperator(
    task_id = "send_telegram_alert",
    python_callable = send_telegram_alert,
    dag = dag
)




task_fetch_crypto >> task_save_crypto >> task_soda_crypto >> task_branching

task_branching >> [task_fix_price, task_fix_doublons, task_final_ok]
task_fix_price >> task_final_ok
task_fix_doublons >> task_final_ok

task_final_ok >> task_is_it_time

task_is_it_time >> task_fetch_news >> task_save_news >> task_soda_news >> task_gemini >> task_telegram