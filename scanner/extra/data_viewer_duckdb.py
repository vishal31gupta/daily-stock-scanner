import duckdb

df = duckdb.sql("""
    SELECT *
    FROM 'data/market_data/2026-10-08/NIFTY_OPTIONS/*.parquet'
    LIMIT 20
""").df()

print(df)


SELECT
    tradingsymbol,
    COUNT(*) AS candles
FROM 'data/market_data/2026-10-08/NIFTY_OPTIONS/*.parquet'
GROUP BY tradingsymbol
ORDER BY candles DESC;
