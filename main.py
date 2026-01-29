import os
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# 1. Setup Logging
# This tells Python: "Save all messages INFO level and above to 'pipeline.log'"
logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()

def run_pipeline():
    logging.info("Starting the Data Pipeline...")
    
    # 2. Fetch Data (with Error Handling)
    api_key = "515476f81db54e839dab597ef45677b7" # OR os.getenv("NEWS_API_KEY") if you set it
    url = f"https://newsapi.org/v2/everything?q=AI&apiKey={api_key}"
    
    try:
        logging.info(f"Fetching data from NewsAPI...")
        response = requests.get(url, timeout=10) # 10 second timeout limit
        response.raise_for_status() # Raises an error if status is 400/404/500
        data = response.json()
        articles = data.get('articles', [])
        logging.info(f"Successfully fetched {len(articles)} articles.")
        
        if not articles:
            logging.warning("No articles found. Stopping pipeline.")
            return

    except requests.exceptions.RequestException as e:
        logging.error(f"API Request failed: {e}")
        return # Stop the function if API fails

    # 3. Process Data
    try:
        df = pd.DataFrame(articles)
        
        # Cleaning logic
        df['publishedAt'] = pd.to_datetime(df['publishedAt'])
        df = df.dropna(subset=['title', 'description'])
        df['source_name'] = df['source'].apply(lambda x: x['name'] if isinstance(x, dict) else None)
        df = df.drop(columns=['source'])
        
        logging.info("Data cleaning complete.")

    except Exception as e:
        logging.error(f"Data Processing failed: {e}")
        return

    # 4. Upload to Database
    try:
        db_user = os.getenv("DB_USER")
        db_pass = os.getenv("DB_PASS")
        db_host = os.getenv("DB_HOST")
        db_name = os.getenv("DB_NAME")
        
        # Check if credentials exist
        if not all([db_user, db_pass, db_host, db_name]):
            logging.error("Database credentials missing in .env file.")
            return

        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}"
        engine = create_engine(db_url)
        
        # Upload
        df.to_sql('news_articles', engine, if_exists='append', index=False)
        logging.info("Successfully uploaded data to AWS RDS.")
        
    except SQLAlchemyError as e:
        logging.error(f"Database Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected Error during upload: {e}")

if __name__ == "__main__":
    run_pipeline()
    logging.info("Pipeline finished.\n")