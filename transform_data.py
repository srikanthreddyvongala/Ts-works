# transform_data.py
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///movies.db', echo=False)

# Read all rows from movies table
df = pd.read_sql('movies', engine)

# Transformation:
# Add a new column 'decade' and filter by rating > 8.5
df['decade'] = (df['release_year'] // 10) * 10
df_filtered = df[df['rating'] > 8.5]

# Save transformed data to a new table
df_filtered.to_sql('transformed_movies', engine, if_exists='replace', index=False)

print("✅ Transformation complete! Data saved to 'transformed_movies' table.")
