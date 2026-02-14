import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime, timedelta

# -----------------------
# Page Config
# -----------------------
st.set_page_config(
    page_title="Batch Analytics Dashboard",
    layout="wide"
)
st.markdown("""
<style>
body {
    background-color: #F8F9FA;
}
</style>
""", unsafe_allow_html=True)


st.markdown("<h1 style='text-align:center;'>Dashboard</h1>", unsafe_allow_html=True)

# -----------------------
# Database Connection
# -----------------------
@st.cache_resource
def get_connection():
    return psycopg2.connect(st.secrets["Neon_key"])

conn = get_connection()


# Convert today's date to YYYYMMDD integer
prev_day = datetime.today() - timedelta(days=1)
today_int = int(prev_day.strftime('%Y%m%d'))

@st.cache_data(ttl=600)
def get_kpis(today_int, selected_channel):
    conn = psycopg2.connect(st.secrets["Neon_key"]) 
    base_query = """
        FROM fact_transactions f
        JOIN dim_channel c ON f.channel_key = c.channel_key
        WHERE f.date_key = %s
    """

    params = [today_int]

    if selected_channel != "All":
        base_query += " AND c.channel_name = %s"
        params.append(selected_channel)

    daily_revenue_query = f"""
        SELECT SUM(f.amount)
        {base_query}
        AND f.status = 'success'
    """

    failure_rate_query = f"""
        SELECT COUNT(*) FILTER (WHERE f.status='failed')::float
               / NULLIF(COUNT(*),0)
        {base_query}
    """

    avg_proc_query = f"""
        SELECT AVG(f.processing_time)
        {base_query}
    """

    daily_revenue = pd.read_sql(daily_revenue_query, conn, params=params).iloc[0,0] or 0
    failure_rate = pd.read_sql(failure_rate_query, conn, params=params).iloc[0,0] or 0
    avg_processing_time = pd.read_sql(avg_proc_query, conn, params=params).iloc[0,0] or 0
    conn.close()
    return daily_revenue, failure_rate, avg_processing_time

channel_list_query = """
    SELECT channel_name
    FROM dim_channel
    ORDER BY channel_name
"""
channels_df = pd.read_sql(channel_list_query, conn)

channel_options = ["All"] + channels_df["channel_name"].tolist()

selected_channel = st.sidebar.selectbox(
    "Channel",
    channel_options
)
st.caption(f"Showing data for: {selected_channel}")

channel_title = selected_channel if selected_channel != "All" else "All Channels"



# -----------------------
# KPI Queries
# -----------------------

daily_revenue, failure_rate, avg_processing_time = get_kpis(today_int, selected_channel)

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
            <h2 style="color:#2E7D32">₹{daily_revenue:,.2f}</h2>
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
    JOIN dim_channel c ON f.channel_key = c.channel_key
    WHERE f.status='success'
"""
params = []

if selected_channel != "All":
    revenue_trend_query += " AND c.channel_name = %s"
    params.append(selected_channel)

revenue_trend_query += """
    GROUP BY d.full_date
    ORDER BY d.full_date
"""

revenue_trend = pd.read_sql(revenue_trend_query, conn, params=params)
revenue_trend['7_day_avg'] = revenue_trend['total_revenue'].rolling(7).mean()


fig_rev = px.line(
    revenue_trend, 
    x='full_date', 
    y='total_revenue', 
    title=f"Revenue Trend for {channel_title}",
    markers=True
)
fig_rev.add_scatter(
    x=revenue_trend['full_date'],
    y=revenue_trend['7_day_avg'],
    mode='lines',
    name='7 Day Avg',
    line=dict(width=3, dash='solid')
)
fig_rev.update_traces(
    hovertemplate="₹%{y:,.0f}<extra></extra>"
)
fig_rev.update_layout(
    plot_bgcolor='#F8F9FA', 
    paper_bgcolor='#F8F9FA', 
    font=dict(color="#212121")
)
fig_rev.update_yaxes(tickprefix="₹", separatethousands=True)
st.plotly_chart(fig_rev, use_container_width=True)

# -----------------------
# Failure Rate Trend
# -----------------------
failure_trend_query = """
    SELECT d.full_date, 
           COUNT(*) FILTER (WHERE f.status='failed')::float / NULLIF(COUNT(*),0) AS failure_rate
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_channel c ON f.channel_key = c.channel_key
    WHERE 1=1
"""

params = []

if selected_channel != "All":
    failure_trend_query += " AND c.channel_name = %s"
    params.append(selected_channel)

failure_trend_query += """
    GROUP BY d.full_date
    ORDER BY d.full_date
"""
failure_trend = pd.read_sql(failure_trend_query, conn, params=params)
failure_trend['failure_rate'] = (failure_trend['failure_rate'] * 100).round(0)

fig_fail = px.line(
    failure_trend,
    x='full_date',
    y='failure_rate',
    title=f"Failure Rate Trend for {channel_title}",
    markers=True
)
fig_fail.update_layout(
    plot_bgcolor='#F8F9FA', 
    paper_bgcolor='#F8F9FA', 
    font=dict(color="#212121"),
    yaxis_title="Failure Rate (%)"
)
fig_fail.update_yaxes(suffix="%")
st.plotly_chart(fig_fail, use_container_width=True)

# -----------------------
# Channel Comparison
# -----------------------
if selected_channel == "All":
    # show comparison
    channel_fail_query = """
        SELECT c.channel_name,
               COUNT(*) FILTER (WHERE f.status='failed')::float 
               / NULLIF(COUNT(*),0) AS failure_rate
        FROM fact_transactions f
        JOIN dim_channel c ON f.channel_key = c.channel_key
        GROUP BY c.channel_name
    """

    channel_fail = pd.read_sql(channel_fail_query, conn)
    channel_fail['failure_rate'] = (channel_fail['failure_rate'] * 100).round(0)
    channel_fail = channel_fail.sort_values(by='failure_rate', ascending=False)

    fig_chan = px.bar(
        channel_fail,
        x='channel_name',
        y='failure_rate',
        title="Failure Rate by Channel",
        text=channel_fail['failure_rate'].astype(int).astype(str) + '%',
        color='failure_rate',
        color_continuous_scale='reds'
    )

    fig_chan.update_traces(textposition='inside')
    fig_chan.update_layout(
        yaxis_title="Failure Rate (%)",
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121")
    )

    st.plotly_chart(fig_chan, use_container_width=True)




