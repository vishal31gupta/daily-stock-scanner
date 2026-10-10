import pandas as pd

df = pd.read_parquet(
    "your_file.parquet"
)

print(df.head())
print(df.shape)
print(df.columns)