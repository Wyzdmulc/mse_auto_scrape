# ==============================
# Malawi Stock Exchange Analysis
# ==============================

import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns

# --- 1️⃣ Load Data ---
conn = sqlite3.connect("mse_data.db")
df = pd.read_sql("SELECT * FROM daily_data", conn)
conn.close()

# --- 2️⃣ Clean Data ---
df['Date'] = pd.to_datetime(df['Date'])
df = df.sort_values(['Ticker', 'Date'])
df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
df.dropna(subset=['Close'], inplace=True)

# --- 3️⃣ Calculate Metrics ---

# Daily % Change
df['Daily_Return_%'] = df.groupby('Ticker')['Close'].pct_change() * 100

# 20-day Moving Average
df['MA_20'] = df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(20, min_periods=1).mean())

# Volatility (rolling 20-day standard deviation)
df['Volatility_20'] = df.groupby('Ticker')['Daily_Return_%'].transform(lambda x: x.rolling(20, min_periods=1).std())

# Cumulative Return (from first recorded day)
df['Cumulative_Return'] = df.groupby('Ticker')['Close'].transform(lambda x: x / x.iloc[0] - 1)

# 7-day Volume Trend (rolling mean)
df['Vol_MA_7'] = df.groupby('Ticker')['Volume'].transform(lambda x: x.rolling(7, min_periods=1).mean())

# --- 4️⃣ Save Updated Data ---
df.to_excel("mse_analysis_output.xlsx", index=False)

# --- 5️⃣ Visualization Setup ---
sns.set(style="whitegrid", palette="tab10")
plt.figure(figsize=(12,6))

# Example Ticker — you can loop or select
example_ticker = df['Ticker'].unique()[0]
ticker_df = df[df['Ticker'] == example_ticker]

# --- 6️⃣ Plot: Closing Price & Moving Average ---
plt.plot(ticker_df['Date'], ticker_df['Close'], label="Close Price", linewidth=2)
plt.plot(ticker_df['Date'], ticker_df['MA_20'], label="20-Day MA", linestyle="--")
plt.title(f"{example_ticker} Price Trend (with 20-Day MA)")
plt.xlabel("Date")
plt.ylabel("Price (MWK)")
plt.legend()
plt.tight_layout()
plt.show()

# --- 7️⃣ Plot: Volatility ---
plt.figure(figsize=(12,5))
plt.plot(ticker_df['Date'], ticker_df['Volatility_20'], color="orange", label="Volatility (20-Day)")
plt.title(f"{example_ticker} Rolling Volatility")
plt.xlabel("Date")
plt.ylabel("Volatility (%)")
plt.legend()
plt.tight_layout()
plt.show()

# --- 8️⃣ Plot: Cumulative Return ---
plt.figure(figsize=(12,5))
for ticker in df['Ticker'].unique():
    temp = df[df['Ticker'] == ticker]
    plt.plot(temp['Date'], temp['Cumulative_Return'], label=ticker)
plt.title("Cumulative Returns by Ticker")
plt.xlabel("Date")
plt.ylabel("Cumulative Return (%)")
plt.legend()
plt.tight_layout()
plt.show()

# --- 9️⃣ Summary Output ---
summary = df.groupby('Ticker').agg({
    'Daily_Return_%': 'mean',
    'Volatility_20': 'mean',
    'Cumulative_Return': 'last',
    'Volume': 'mean'
}).rename(columns={
    'Daily_Return_%': 'Avg Daily Return (%)',
    'Volatility_20': 'Avg Volatility (%)',
    'Cumulative_Return': 'Total Return',
    'Volume': 'Avg Volume'
}).reset_index()

print("\n=== Summary Performance ===")
print(summary.round(2))
