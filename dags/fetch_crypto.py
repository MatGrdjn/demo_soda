import requests 
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional
import os 
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)

def fetch_crypto_data() -> Optional[pd.DataFrame]:
    """
    Récupère les prix et volume 24h du Bitcoin, Ethereum et Solana depuis CoinGecko.
    """

    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=eur&include_24hr_vol=true"

    response = requests.get(url)

    if response.status_code == 200:
        raw_data = response.json()
        print("Données reçues avec succès.")
    else:
        print(f"Erreur lors de l'appel API : {response.status_code}")
        return None
    
    df = pd.DataFrame(raw_data).T.reset_index() #Comme ça on a les cryptos lignes par lignes
    df.columns = ["crypto_id", "price_euro", "volume_24h"]

    print(df)
    return df

def save_to_database(df: pd.DataFrame) -> None:
    """
    Envoie les données à la base de donnée postgre dans notre conteneur docker.
    """

    engine = create_engine("postgresql://admin:admin@postgres:5432/airflow_db")

    df["timestamp"] = pd.Timestamp.now()

    df.to_sql("crypto_prices", engine, if_exists="append", index=False)
    print("Données sauvegardées avec succès.")


def fetch_crypto_news() -> Optional[pd.DataFrame]:
    """Récupère les news crypto française sur le bitcoin"""
    api_key = os.environ.get("NEWSDATA_API_KEY")
    url = f"https://newsdata.io/api/1/crypto?apikey={api_key}&q=bitcoin&language=fr&timezone=europe/paris&coin=btc"

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        articles = data.get("results", [])

        news_list = []
        for article in articles:
            news_list.append({
                "title" : article.get("title"),
                "description" : article.get("description"),
                "source" : article.get("source_id"),
                "pub_date" : article.get("pubDate")
            })
        
        df_news = pd.DataFrame(news_list)

        print("Données reçues avec succès.")
        print(df_news)

        return df_news
    else:
        print("Erreur API News:", response.status_code)
        return None

def save_news_to_database(df_news: pd.DataFrame) -> None:
    engine = create_engine("postgresql://admin:admin@postgres:5432/airflow_db")
    df_news.to_sql("crypto_news", engine, if_exists="replace", index=False) #Mettre append pour une utilisation sur la semaine
    print("Données sauvegardées avec succès.")

if __name__ == "__main__":
    df = fetch_crypto_news()
    if df is not None:
        save_news_to_database(df)