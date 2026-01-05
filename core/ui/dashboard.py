import streamlit as st
import pandas as pd
import clickhouse_connect
from loguru import logger

CH_HOST = "clickhouse"
CH_USER = "admin"
CH_PASS = "password"
CH_DB = "glasshouse"

st.set_page_config(page_title="Glasshouse Monitor", layout="wide")
st.title("Glasshouse:Real-Time Market & Sentiment")

@st.cache_resource
def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        username=CH_USER,
        password=CH_PASS,
        database=CH_DB
    )

def fetch_data():
    client = get_client()
    sent_query = """
    SELECT 
        timestamp,
        sentiment_score,
        headline,
        sentiment_label
    FROM 
        glasshouse.social_enriched
    ORDER BY 
        timestamp DESC
    LIMIT 100
    """
    price_query="""
    SELECT
        timestamp,
        price
    FROM glasshouse.trades_silver
    ORDER BY timestamp DESC
    LIMIT 500
    """

    df_sent = client.query_df(sent_query)
    df_price = client.query_df(price_query)
    return df_sent, df_price

try:
    df_p,df_s = fetch_data()
    col1,col2 = st.columns9([2,1])
    with col1:
        st.subheader("Price Movement (BTC/USDT)")
        if not df_p.empty:
            st.line_chart(df_p.set_index('timestamp'))
        else:
            st.info("Waiting for price data...")
    
    with col2:
        st.subheader("Recent Sentiment")
        if not df_s.empty:
            st.dataframe(df_s[['timestamp','headline','sentiment_label']], hide_index=True)
            avg_sent = df_s['sentiment_score'].mean()
            st.metric("Average Sentiment Score (last 100)", f"{avg_sent:.2f}")
        else:
            st.info("Waiting for sentiment scores...")

except Exception as e:
    logger.error(f"Error fetching data: {e}")
    st.error("An error occurred while fetching data. Please try again later.")

if st.button("Refresh Data"):
    st.rerun()