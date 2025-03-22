import smallpond
import time

# Timer function to measure execution time
def measure_time(description):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            print(f"{description}: {end_time - start_time:.2f} seconds")
            return result
        return wrapper
    return decorator

# Initialize session
@measure_time("Session initialization")
def init_smallpond():
    return smallpond.init()

# Load data
@measure_time("Data loading")
def load_data(sp):
    return sp.read_parquet("Top_spotify_songs.parquet")

# Process data
@measure_time("Data repartitioning")
def repartition_data(df):
    return df.repartition(5, hash_by="artists")

@measure_time("SQL transformation")
def transform_data(df, sp):
    return sp.partial_sql("SELECT artists, avg(popularity) avg_popularity FROM {0} GROUP BY artists ORDER BY avg_popularity DESC", df)

# Save results
@measure_time("Writing results")
def save_results(df):
    df.write_parquet("output/")

# Convert to pandas for display
@measure_time("Converting to pandas")
def convert_to_pandas(df):
    return df.to_pandas()

# Main execution
start_total = time.time()

sp = init_smallpond()
df = load_data(sp)
df = repartition_data(df)
df = transform_data(df, sp)
save_results(df)
pandas_df = convert_to_pandas(df)

end_total = time.time()
print(f"Total execution time: {end_total - start_total:.2f} seconds")

# Show results
print("\nResults:")
print(pandas_df)