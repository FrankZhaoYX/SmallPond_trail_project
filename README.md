# SmallPond Project

## Introduction

This repository is a comparison of SmallPond framework and PySpark using a small dataset. The goal is to evaluate performance differences between these two data processing frameworks on identical operations.

The dataset used comes from [Kaggle: Top Spotify Songs in 73 Countries](https://www.kaggle.com/datasets/anandshaw2001/top-spotify-songs-in-73-countries/data).

This project was inspired by the article [DeepSeek SmallPond: A Game-Changer for Data Engineers Seeking Lightweight Solutions](https://medium.com/data-engineering-space/deepseek-smallpond-a-game-changer-for-data-engineers-seeking-lightweight-solutions-365c21cbcdaa).

## Performance Comparison

I evaluated the time difference between SmallPond and PySpark based on the same query:
```sql
SELECT artists, avg(popularity) as avg_popularity 
FROM spotify 
GROUP BY artists 
ORDER BY avg_popularity DESC
```

### Results

#### PySpark Performance
```
Session initialization: 4.88 seconds
Data loading: 4.31 seconds                                                      
Data repartitioning: 0.09 seconds
SQL transformation: 0.76 seconds
Writing results: 7.54 seconds                                                   
Converting to pandas: 2.29 seconds                                              
Total execution time: 19.88 seconds
```

#### SmallPond Performance
```
Session initialization: 7.72 seconds
Data loading: 0.00 seconds
Data repartitioning: 0.00 seconds
SQL transformation: 0.00 seconds
Writing results: 6.50 seconds
Converting to pandas: 4.47 seconds
Total execution time: 18.70 seconds
```

## Key Observations

- SmallPond has a slightly faster total execution time (18.70s vs 19.88s)
- PySpark is faster at session initialization but slower at data loading
- Data processing (repartitioning and SQL transformation) appears to be instantaneous in SmallPond
- SmallPond has slightly faster write operations but slower pandas conversion

## Setup and Execution

Both frameworks were tested on the same machine using identical data and operations. The code for both implementations includes timing decorators to measure the performance of each operation precisely.