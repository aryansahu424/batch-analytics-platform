import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime

# -----------------------
# Page Config
# -----------------------
st.set_page_config(
    page_title="Bath Analytics Dashboard",
    layout="wide"
)

st.markdown("<h1 style='text-align:center;'>Dashboard</h1>", unsafe_allow_html=True)

# -----------------------
# Database Connection
# -----------------------
Neon_key = st.secrets["Neon_key"]
conn = psycopg2.connect(Neon_key)

# Convert today's date to YYYYMMDD integer
today_int = int(datetime.today().strftime('%Y%m%d'))

# -----------------------
# KPI Queries
# -----------------------
daily_revenue_query = """
    SELECT SUM(amount) AS total_revenue
    FROM fact_transactions
    WHERE status='success' AND date_key = %s
"""
failure_rate_query = """
    SELECT COUNT(*) FILTER (WHERE status='failed')::float / COUNT(*) AS failure_rate
    FROM fact_transactions
    WHERE date_key = %s
"""
avg_proc_query = """
    SELECT AVG(processing_time) AS avg_processing_time
    FROM fact_transactions
    WHERE date_key = %s
"""

daily_revenue = pd.read_sql(daily_revenue_query, conn, params=[today_int]).iloc[0,0] or 0
failure_rate = pd.read_sql(failure_rate_query, conn, params=[today_int]).iloc[0,0] or 0
avg_processing_time = pd.read_sql(avg_proc_query, conn, params=[today_int]).iloc[0,0] or 0

# -----------------------
# KPI Cards Layout
# -----------------------
st.markdown("## Daily KPIs")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        f"""
        <div style="padding:20px; border-radius:10px; background-color:#E8F5E9; text-align:center; box-shadow: 2px 2px 10px rgba(0,0,0,0.1);">
            <h4>Daily Revenue</h4>
            <h2 style="color:#2E7D32">${daily_revenue:,.2f}</h2>
        </div>
        """, unsafe_allow_html=True
    )

with col2:
    st.markdown(
        f"""
        <div style="padding:20px; border-radius:10px; background-color:#FFEBEE; text-align:center; box-shadow: 2px 2px 10px rgba(0,0,0,0.1);">
            <h4>⚠️ Failure Rate</h4>
            <h2 style="color:#C62828">{failure_rate:.2%}</h2>
        </div>
        """, unsafe_allow_html=True
    )

with col3:
    st.markdown(
        f"""
        <div style="padding:20px; border-radius:10px; background-color:#E3F2FD; text-align:center; box-shadow: 2px 2px 10px rgba(0,0,0,0.1);">
            <h4>⏱ Avg Processing Time</h4>
            <h2 style="color:#1565C0">{avg_processing_time:.2f} sec</h2>
        </div>
        """, unsafe_allow_html=True
    )

# -----------------------
# Revenue Trend
# -----------------------
revenue_trend_query = """
    SELECT d.full_date, SUM(f.amount) AS total_revenue
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.status='success'
    GROUP BY d.full_date
    ORDER BY d.full_date
"""
revenue_trend = pd.read_sql(revenue_trend_query, conn)

fig_rev = px.line(
    revenue_trend, 
    x='full_date', 
    y='total_revenue', 
    title="Revenue Trend",
    markers=True
)
fig_rev.update_layout(
    plot_bgcolor='#F8F9FA', 
    paper_bgcolor='#F8F9FA', 
    font=dict(color="#212121")
)
st.plotly_chart(fig_rev, use_container_width=True)

# -----------------------
# Failure Rate Trend
# -----------------------
failure_trend_query = """
    SELECT d.full_date, 
           COUNT(*) FILTER (WHERE f.status='failed')::float / COUNT(*) AS failure_rate
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.full_date
    ORDER BY d.full_date
"""
failure_trend = pd.read_sql(failure_trend_query, conn)

fig_fail = px.line(
    failure_trend,
    x='full_date',
    y='failure_rate',
    title="Failure Rate Trend",
    markers=True
)
fig_fail.update_layout(
    plot_bgcolor='#F8F9FA', 
    paper_bgcolor='#F8F9FA', 
    font=dict(color="#212121")
)
st.plotly_chart(fig_fail, use_container_width=True)

# -----------------------
# Channel Comparison
# -----------------------
channel_fail_query = """
    SELECT c.channel_name,
           COUNT(*) FILTER (WHERE f.status='failed')::float / COUNT(*) AS failure_rate
    FROM fact_transactions f
    JOIN dim_channel c ON f.channel_key = c.channel_key
    GROUP BY c.channel_name
"""
channel_fail = pd.read_sql(channel_fail_query, conn)

fig_chan = px.bar(
    channel_fail,
    x='channel_name',
    y='failure_rate',
    title="Failure Rate by Channel",
    text='failure_rate',
    color='failure_rate',
    color_continuous_scale='reds'
)
fig_chan.update_layout(
    plot_bgcolor='#F8F9FA', 
    paper_bgcolor='#F8F9FA', 
    font=dict(color="#212121")
)
st.plotly_chart(fig_chan, use_container_width=True)
