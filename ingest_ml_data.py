# ingest_ml_data.py
import pandas as pd
from sqlalchemy import create_engine

# Connect to SQLite
engine = create_engine('sqlite:///movies.db', echo=False)

# Read the CSV files
movies_df = pd.read_csv('movies.csv')
ratings_df = pd.read_csv('ratings.csv')

# Load into the database
movies_df.to_sql('movies', engine, if_exists='replace', index=False)
ratings_df.to_sql('ratings', engine, if_exists='replace', index=False)

print(f"✅ Loaded {len(movies_df)} movies and {len(ratings_df)} ratings into database.")
