# ingest_data.py
import pandas as pd
from sqlalchemy import create_engine

# Connect to SQLite DB
engine = create_engine('sqlite:///movies.db', echo=False)

# Sample data (you can replace with API or CSV later)
data = [
    {"title": "Inception", "genre": "Sci-Fi", "release_year": 2010, "rating": 8.8},
    {"title": "Interstellar", "genre": "Adventure", "release_year": 2014, "rating": 8.6},
    {"title": "The Dark Knight", "genre": "Action", "release_year": 2008, "rating": 9.0}
]

# Convert to DataFrame
df = pd.DataFrame(data)

# Load into DB table
df.to_sql('movies', engine, if_exists='append', index=False)

print("✅ Data inserted successfully into 'movies' table.")
