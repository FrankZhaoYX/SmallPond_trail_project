import pandas as pd

# Read the CSV file
df = pd.read_csv('Top_spotify_songs.csv')

# Write to parquet format
df.to_parquet('Top_spotify_songs.parquet')