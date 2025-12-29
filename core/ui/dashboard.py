import streamlit as st
import clickhouse_connect
import pandas as pd
import time
from datetime import datetime, timedelta

st.set_page_config(page_title="Glasshouse Live Monitor", layout="wide")

# Connect to ClickHouse
@st.cache_resource
def get_ch_client():
    return clickhouse_connect.get_client(host='clickhouse', port=8123, username='admin', password='password')

client = get_ch_client()

st.title("Glasshouse: Price vs. Sentiment")

# Sidebar for controls
refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 1, 10, 5)

# Layout: Two columns for KPIs
col1, col2, col3 = st.columns(3)

def fetch_data():
    # Fetch Price Data (Last 5 mins)
    price_query = "SELECT timestamp, price FROM glasshouse.trades_silver WHERE timestamp > now() - INTERVAL 5 MINUTE ORDER BY timestamp DESC"
    df_price = client.query_df(price_query)
    
    # Fetch Sentiment Data (Last 5 mins)
    sent_query = "SELECT created_utc as timestamp, sentiment_score FROM glasshouse.social_enriched WHERE created_utc > now() - INTERVAL 5 MINUTE ORDER BY created_utc DESC"
    df_sent = client.query_df(sent_query)
    
    return df_price, df_sent

# Main Loop
placeholder = st.empty()

while True:
    df_p, df_s = fetch_data()
    
    with placeholder.container():
        # 1. Update Metrics
        avg_price = df_p['price'].mean() if not df_p.empty else 0
        avg_sent = df_s['sentiment_score'].mean() if not df_s.empty else 0.5
        
        col1.metric("Live BTC Price", f"${avg_price:,.2f}")
        col2.metric("Market Sentiment", f"{avg_sent:.2f}", delta=f"{avg_sent - 0.5:.2f}")
        col3.metric("Last Data Sync", datetime.now().strftime("%H:%M:%S"))

        # 2. Visualization
        st.subheader("Price Movement (Last 5 Minutes)")
        st.line_chart(df_p.set_index('timestamp'))

        st.subheader("Latest Scored News")
        if not df_s.empty:
            st.table(df_s.head(5))
        else:
            st.info("Waiting for sentiment scores...")

    time.sleep(refresh_rate)