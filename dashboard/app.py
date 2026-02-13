import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# Connect to your DB
Neon_key = st.secrets["Neon_key"]
st.write("API Key is loaded!")  # Don't print real secrets

conn = psycopg2.connect(Neon_key)

# KPI Cards
daily_revenue = pd.read_sql("""
    SELECT SUM(amount) AS total_revenue
    FROM fact_transactions
    WHERE status='success' AND date_key = CURRENT_DATE
""", conn).iloc[0,0]

failure_rate = pd.read_sql("""
    SELECT COUNT(*) FILTER (WHERE status='failed')::float / COUNT(*) AS failure_rate
    FROM fact_transactions
    WHERE date_key = CURRENT_DATE
""", conn).iloc[0,0]

avg_processing_time = pd.read_sql("""
    SELECT AVG(processing_time) AS avg_processing_time
    FROM fact_transactions
    WHERE date_key = CURRENT_DATE
""", conn).iloc[0,0]

st.metric("Daily Revenue", f"${daily_revenue:,.2f}")
st.metric("Failure Rate", f"{failure_rate:.2%}")
st.metric("Avg Processing Time", f"{avg_processing_time:.2f} sec")

# Revenue Trend
revenue_trend = pd.read_sql("""
    SELECT d.full_date, SUM(f.amount) AS total_revenue
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.status='success'
    GROUP BY d.full_date
    ORDER BY d.full_date
""", conn)
fig_rev = px.line(revenue_trend, x='full_date', y='total_revenue', title="Revenue Trend")
st.plotly_chart(fig_rev)

# Failure Rate Trend
failure_trend = pd.read_sql("""
    SELECT d.full_date, COUNT(*) FILTER (WHERE f.status='failed')::float / COUNT(*) AS failure_rate
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.full_date
    ORDER BY d.full_date
""", conn)
fig_fail = px.line(failure_trend, x='full_date', y='failure_rate', title="Failure Rate Trend")
st.plotly_chart(fig_fail)

# Channel Comparison
channel_fail = pd.read_sql("""
    SELECT c.channel_name,
           COUNT(*) FILTER (WHERE f.status='failed')::float / COUNT(*) AS failure_rate
    FROM fact_transactions f
    JOIN dim_channel c ON f.channel_key = c.channel_key
    GROUP BY c.channel_name
""", conn)
fig_chan = px.bar(channel_fail, x='channel_name', y='failure_rate', title="Failure Rate by Channel")
st.plotly_chart(fig_chan)
