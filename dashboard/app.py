import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime, timedelta

# -----------------------
# Page Config
# -----------------------
st.set_page_config(
    page_title="Revenue Analytics Dashboard",
    layout="wide"
)
st.markdown("""
<style>
body {
    background-color: #F8F9FA;
}
.stPlotlyChart {
    background: rgba(255, 255, 255, 0.7);
    backdrop-filter: blur(10px);
    border-radius: 12px;
    border: 1px solid rgba(255, 255, 255, 0.3);
    box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.1);
    padding: 10px;
}
.stPlotlyChart > div {
    border-radius: 10px;
    overflow: hidden;
}
.js-plotly-plot .plotly {
    border-radius: 10px;
}
</style>
""", unsafe_allow_html=True)


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
def get_filter_options(column, table="dim_city", where_clause="", params_tuple=()):
    params = list(params_tuple)
    query = f"SELECT DISTINCT {column} FROM {table}"
    if where_clause:
        query += " WHERE " + where_clause
    query += f" ORDER BY {column}"
    df = pd.read_sql(query, conn, params=params)
    return ["All"] + df[column].tolist()

@st.cache_data(ttl=600)
def get_cascading_city_filters(selected_city=None, selected_state=None, selected_region=None):
    """Get cascading filter options for city/state/region"""
    where_parts = []
    params = []
    
    if selected_region and selected_region != "All":
        where_parts.append("region = %s")
        params.append(selected_region)
    if selected_state and selected_state != "All":
        where_parts.append("state = %s")
        params.append(selected_state)
    if selected_city and selected_city != "All":
        where_parts.append("city_name = %s")
        params.append(selected_city)
    
    where_clause = " AND ".join(where_parts) if where_parts else ""
    
    cities = get_filter_options("city_name", "dim_city", where_clause, tuple(params))
    states = get_filter_options("state", "dim_city", where_clause, tuple(params))
    regions = get_filter_options("region", "dim_city", where_clause, tuple(params))
    
    return cities, states, regions

def build_filter_clause(filters_dict):
    clauses = []
    params = []

    mapping = {
        "channel": "c.channel_name",
        "region": "ci.region",
        "state": "ci.state",
        "city": "ci.city_name",
        "segment": "cu.segment"
    }

    for key, value in filters_dict.items():
        if value != "All":
            clauses.append(f"{mapping[key]} = %s")
            params.append(value)

    filter_clause = " AND " + " AND ".join(clauses) if clauses else ""
    return filter_clause, params

# -----------------------
# KPI Queries
# -----------------------
@st.cache_data(ttl=600)
def get_kpis_for_date_int(date_int, filter_clause, params_tuple=()):
    params = list(params_tuple)
    query = f"""
    SELECT
        SUM(CASE WHEN f.status='success' THEN f.amount ELSE 0 END) AS total_revenue,
        COUNT(*) FILTER (WHERE f.status='failed')::float / NULLIF(COUNT(*),0) AS failure_rate,
        AVG(f.processing_time) AS avg_processing_time
    FROM fact_transactions f
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key = %s
    {filter_clause}
    """
    query_params = [date_int] + params
    df = pd.read_sql(query, conn, params=query_params)
    
    if df.empty:
        return 0, 0, 0
    else:
        return (
            df['total_revenue'].iloc[0] or 0,
            df['failure_rate'].iloc[0] or 0,
            df['avg_processing_time'].iloc[0] or 0
        )

@st.cache_data(ttl=600)
def get_kpis_for_range(start_date, end_date, filter_clause, params_tuple=()):
    params = list(params_tuple)
    query = f"""
    SELECT
        SUM(CASE WHEN f.status='success' THEN f.amount ELSE 0 END) AS total_revenue,
        COUNT(*) FILTER (WHERE f.status='failed')::float / NULLIF(COUNT(*),0) AS failure_rate,
        AVG(f.processing_time) AS avg_processing_time
    FROM fact_transactions f
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    """
    query_params = [int(start_date.strftime("%Y%m%d")), int(end_date.strftime("%Y%m%d"))] + params
    df = pd.read_sql(query, conn, params=query_params)
    
    if df.empty:
        return 0, 0, 0
    else:
        return (
            df['total_revenue'].iloc[0] or 0,
            df['failure_rate'].iloc[0] or 0,
            df['avg_processing_time'].iloc[0] or 0
        )

@st.cache_data(ttl=600)
def get_trend_data(start_date_int, end_date_int, filter_clause, params_tuple=()):
    """Get all trend data in one query"""
    params = list(params_tuple)
    query = f"""
    SELECT d.full_date,
           SUM(CASE WHEN f.status='success' THEN f.amount ELSE 0 END) AS revenue,
           COUNT(*) FILTER (WHERE f.status='failed')::float / NULLIF(COUNT(*),0) * 100 AS failure_rate,
           AVG(f.processing_time) AS avg_processing_time
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    GROUP BY d.full_date
    ORDER BY d.full_date
    """
    return pd.read_sql(query, conn, params=[start_date_int, end_date_int] + params)

@st.cache_data(ttl=600)
def get_breakdown_data(start_date_int, end_date_int, breakdown_column, filter_clause, params_tuple=()):
    """Get breakdown trend data"""
    params = list(params_tuple)
    query = f"""
    SELECT d.full_date, {breakdown_column} as breakdown_value,
           SUM(CASE WHEN f.status='success' THEN f.amount ELSE 0 END) AS revenue,
           COUNT(*) FILTER (WHERE f.status='failed')::float / NULLIF(COUNT(*),0) * 100 AS failure_rate,
           AVG(f.processing_time) AS avg_processing_time
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    GROUP BY d.full_date, {breakdown_column}
    ORDER BY d.full_date
    """
    return pd.read_sql(query, conn, params=[start_date_int, end_date_int] + params)

@st.cache_data(ttl=600)
def get_comparison_data(start_date_int, end_date_int, dimension_column, filter_clause, params_tuple=(), limit=None):
    """Get comparison chart data"""
    params = list(params_tuple)
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
    SELECT {dimension_column} as dimension,
           COUNT(*) FILTER (WHERE f.status='failed')::float / NULLIF(COUNT(*),0) * 100 AS failure_rate,
           AVG(f.processing_time) AS avg_processing_time
    FROM fact_transactions f
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    GROUP BY {dimension_column}
    ORDER BY failure_rate DESC
    {limit_clause}
    """
    return pd.read_sql(query, conn, params=[start_date_int, end_date_int] + params)

# -----------------------
# Header with Filter Selector on Right
# -----------------------
col_left, col_right = st.columns([5, 1])

with col_left:
    st.markdown("<h1 style='text-align:left;'>Revenue Analytics Dashboard</h1>", unsafe_allow_html=True)

with col_right:
    filter_options = ["Date", "City", "State", "Region", "Channel", "Segment"]
    selected_filters = st.multiselect("", filter_options, key="filter_selector", placeholder="Select Filters")

# Initialize selected_channel
selected_channel = "All"

filters_dict = {}

# -----------------------
# Dynamically render widgets based on selection - horizontally aligned
# -----------------------
if selected_filters:
    # Count number of filters to create appropriate columns
    num_filters = len(selected_filters)
    # Special handling for Date which needs 2 columns
    if "Date" in selected_filters:
        num_filters += 1
    
    # Create dynamic column ratio: left empty space increases as filters decrease
    left_ratio = 8 - num_filters
    right_ratio = num_filters
    
    filter_col_empty, filter_col_content = st.columns([left_ratio, right_ratio])
    
    with filter_col_content:
        # Create horizontal columns for all filters
        filter_cols = st.columns(num_filters)
        col_idx = 0
        
        if "Date" in selected_filters:
            with filter_cols[col_idx]:
                start_date = st.date_input("Start Date", prev_day, key="dyn_start_date")
            col_idx += 1
            with filter_cols[col_idx]:
                end_date = st.date_input("End Date", prev_day, key="dyn_end_date")
            col_idx += 1
        else:
            start_date = prev_day
            end_date = prev_day
        
        # Handle cascading city/state/region filters
        temp_city = None
        temp_state = None
        temp_region = None
        
        # First pass: get current selections
        if "City" in selected_filters:
            temp_city = st.session_state.get("city_select", "All")
        if "State" in selected_filters:
            temp_state = st.session_state.get("state_select", "All")
        if "Region" in selected_filters:
            temp_region = st.session_state.get("region_select", "All")
        
        # Get cascading options
        city_options, state_options, region_options = get_cascading_city_filters(temp_city, temp_state, temp_region)
        
        # Render filters
        if "City" in selected_filters:
            with filter_cols[col_idx]:
                filters_dict["city"] = st.selectbox("City", city_options, key="city_select")
            col_idx += 1
        
        if "State" in selected_filters:
            with filter_cols[col_idx]:
                filters_dict["state"] = st.selectbox("State", state_options, key="state_select")
            col_idx += 1
        
        if "Region" in selected_filters:
            with filter_cols[col_idx]:
                filters_dict["region"] = st.selectbox("Region", region_options, key="region_select")
            col_idx += 1
        
        if "Channel" in selected_filters:
            with filter_cols[col_idx]:
                channel_options = get_filter_options("channel_name", table="dim_channel")
                selected_channel = st.selectbox("Channel", channel_options, key="channel_select")
                filters_dict["channel"] = selected_channel
            col_idx += 1
        
        if "Segment" in selected_filters:
            with filter_cols[col_idx]:
                segment_options = get_filter_options("segment", table="dim_customer")
                filters_dict["segment"] = st.selectbox("Customer Segment", segment_options, key="segment_select")
            col_idx += 1
else:
    start_date = prev_day
    end_date = prev_day


# -----------------------
# KPI Queries
# -----------------------
filter_clause, params = build_filter_clause(filters_dict)

# Set channel title for charts
channel_title = selected_channel if selected_channel != "All" else "All Channels"
# -----------------------
# KPI Logic
# -----------------------
# Build filter title
filter_parts = []
if filters_dict.get("city") and filters_dict["city"] != "All":
    filter_parts.append(filters_dict["city"])
if filters_dict.get("state") and filters_dict["state"] != "All":
    filter_parts.append(filters_dict["state"])
if filters_dict.get("region") and filters_dict["region"] != "All":
    filter_parts.append(filters_dict["region"])
if filters_dict.get("channel") and filters_dict["channel"] != "All":
    filter_parts.append(filters_dict["channel"])
if filters_dict.get("segment") and filters_dict["segment"] != "All":
    filter_parts.append(filters_dict["segment"])

filter_suffix = f" - {', '.join(filter_parts)}" if filter_parts else ""

if "Date" in selected_filters:
    
    if start_date == end_date:
        # Single date selected
        selected_date_int = int(start_date.strftime("%Y%m%d"))
        daily_revenue, failure_rate, avg_processing_time = get_kpis_for_date_int(
            selected_date_int, filter_clause, tuple(params)
        )
        kpi_title = f"KPIs for {start_date.strftime('%d-%b-%Y')}{filter_suffix}"
    
    else:
        # Date range selected
        daily_revenue, failure_rate, avg_processing_time = get_kpis_for_range(
            start_date, end_date, filter_clause, tuple(params)
        )
        kpi_title = f"KPIs from {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}{filter_suffix}"

else:
    # Default previous day
    daily_revenue, failure_rate, avg_processing_time = get_kpis_for_date_int(
        today_int, filter_clause, tuple(params)
    )
    kpi_title = f"Daily KPIs{filter_suffix}"

# -----------------------
# KPI Cards Layout
# -----------------------
st.markdown(f"## {kpi_title}")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        f"""
        <div style="padding:20px; border-radius:10px; background-color:#E8F5E9; text-align:center; box-shadow: 2px 2px 10px rgba(0,0,0,0.1);">
            <h4>Total Revenue</h4>
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
# Determine date range for trend (default: last 30 days)
if "Date" not in selected_filters:
    trend_start = prev_day - timedelta(days=29)
    trend_end = prev_day
else:
    trend_start = start_date
    trend_end = end_date

# Determine which breakdown to show
breakdown_config = None
if "City" in selected_filters and filters_dict.get("city") == "All":
    breakdown_config = {"column": "ci.city_name", "label": "city_name", "title": "Top 4 Cities"}
elif "State" in selected_filters and filters_dict.get("state") == "All":
    breakdown_config = {"column": "ci.state", "label": "state", "title": "Top 4 States"}
elif "Region" in selected_filters and filters_dict.get("region") == "All":
    breakdown_config = {"column": "ci.region", "label": "region", "title": "Top 4 Regions"}
elif "Channel" in selected_filters and filters_dict.get("channel") == "All":
    breakdown_config = {"column": "c.channel_name", "label": "channel_name", "title": "Top 4 Channels"}
elif "Segment" in selected_filters and filters_dict.get("segment") == "All":
    breakdown_config = {"column": "cu.segment", "label": "segment", "title": "Top 4 Segments"}

if breakdown_config:
    # Show top 4 breakdown + total revenue
    breakdown_query = f"""
    SELECT d.full_date, {breakdown_config['column']} as breakdown_value,
           SUM(CASE WHEN f.status='success' THEN f.amount ELSE 0 END) AS revenue
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    GROUP BY d.full_date, {breakdown_config['column']}
    ORDER BY d.full_date
    """
    
    breakdown_df = pd.read_sql(breakdown_query, conn, params=[int(trend_start.strftime("%Y%m%d")), int(trend_end.strftime("%Y%m%d"))] + params)
    
    # Get top 4 by total revenue
    top_4 = breakdown_df.groupby('breakdown_value')['revenue'].sum().nlargest(4).index.tolist()
    
    # Filter for top 4
    top_trend = breakdown_df[breakdown_df['breakdown_value'].isin(top_4)]
    
    # Get total revenue
    total_rev_query = f"""
    SELECT d.full_date,
           SUM(CASE WHEN f.status='success' THEN f.amount ELSE 0 END) AS revenue
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    GROUP BY d.full_date
    ORDER BY d.full_date
    """
    
    total_df = pd.read_sql(total_rev_query, conn, params=[int(trend_start.strftime("%Y%m%d")), int(trend_end.strftime("%Y%m%d"))] + params)
    total_df['breakdown_value'] = 'Total'
    
    # Combine
    combined_df = pd.concat([top_trend, total_df], ignore_index=True)
    
    fig_rev = px.line(
        combined_df,
        x='full_date',
        y='revenue',
        color='breakdown_value',
        title=f"Revenue Trend - {breakdown_config['title']}",
        markers=True
    )
    
    fig_rev.update_traces(hovertemplate="<b>%{fullData.name}</b><br>%{x}<br>₹%{y:,.0f}<extra></extra>")
    fig_rev.update_layout(
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121"),
        xaxis_title="Date",
        yaxis_title="Revenue",
        margin=dict(t=80, b=50, l=50, r=50),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=0.99,
            xanchor="center",
            x=0.5,
            title_text=''
        )
    )
    fig_rev.update_yaxes(tickprefix="₹", separatethousands=True)
    fig_rev.update_xaxes(tickformat="%d-%b")
    
else:
    # Show total revenue + 7-day average
    rev_query = f"""
    SELECT d.full_date,
           SUM(CASE WHEN f.status='success' THEN f.amount ELSE 0 END) AS total_revenue
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    GROUP BY d.full_date
    ORDER BY d.full_date
    """
    
    rev_df = pd.read_sql(rev_query, conn, params=[int(trend_start.strftime("%Y%m%d")), int(trend_end.strftime("%Y%m%d"))] + params)
    rev_df['7_day_avg'] = rev_df['total_revenue'].rolling(7, min_periods=1).mean()
    
    fig_rev = px.line(
        rev_df,
        x='full_date',
        y='total_revenue',
        title=f"Revenue Trend{filter_suffix}",
        markers=True
    )
    fig_rev.add_scatter(
        x=rev_df['full_date'],
        y=rev_df['7_day_avg'],
        mode='lines',
        name='7 Day Avg',
        line=dict(width=3, dash='solid'),
        hovertemplate="<b>7 Day Avg</b><br>%{x}<br>₹%{y:,.0f}<extra></extra>"
    )
    fig_rev.update_traces(hovertemplate="<b>%{fullData.name}</b><br>%{x}<br>₹%{y:,.0f}<extra></extra>")
    fig_rev.update_layout(
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121"),
        xaxis_title="Date",
        yaxis_title="Total Revenue"
    )
    fig_rev.update_yaxes(tickprefix="₹", separatethousands=True)
    fig_rev.update_xaxes(tickformat="%d-%b")

st.plotly_chart(fig_rev, use_container_width=True, config={'displayModeBar': False})

# -----------------------
# Failure Rate Trend
# -----------------------
if breakdown_config:
    # Show top 4 breakdown + total
    fail_breakdown_query = f"""
    SELECT d.full_date, {breakdown_config['column']} as breakdown_value,
           COUNT(*) FILTER (WHERE f.status='failed')::float / NULLIF(COUNT(*),0) * 100 AS failure_rate
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    GROUP BY d.full_date, {breakdown_config['column']}
    ORDER BY d.full_date
    """
    
    fail_breakdown_df = pd.read_sql(fail_breakdown_query, conn, params=[int(trend_start.strftime("%Y%m%d")), int(trend_end.strftime("%Y%m%d"))] + params)
    
    # Get top 4 by average failure rate
    top_4_fail = fail_breakdown_df.groupby('breakdown_value')['failure_rate'].mean().nlargest(4).index.tolist()
    top_fail_trend = fail_breakdown_df[fail_breakdown_df['breakdown_value'].isin(top_4_fail)]
    
    # Get total failure rate
    total_fail_query = f"""
    SELECT d.full_date,
           COUNT(*) FILTER (WHERE f.status='failed')::float / NULLIF(COUNT(*),0) * 100 AS failure_rate
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    GROUP BY d.full_date
    ORDER BY d.full_date
    """
    
    total_fail_df = pd.read_sql(total_fail_query, conn, params=[int(trend_start.strftime("%Y%m%d")), int(trend_end.strftime("%Y%m%d"))] + params)
    total_fail_df['breakdown_value'] = 'Total'
    
    combined_fail_df = pd.concat([top_fail_trend, total_fail_df], ignore_index=True)
    
    fig_fail = px.line(
        combined_fail_df,
        x='full_date',
        y='failure_rate',
        color='breakdown_value',
        title=f"Failure Rate Trend - {breakdown_config['title']}",
        markers=True
    )
    
    fig_fail.update_traces(hovertemplate="<b>%{fullData.name}</b><br>%{x}<br>%{y:.1f}%<extra></extra>")
    fig_fail.update_layout(
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121"),
        yaxis_title="Failure Rate (%)",
        xaxis_title="Date",
        margin=dict(t=80, b=50, l=50, r=50),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=0.99,
            xanchor="center",
            x=0.5,
            title_text=''
        )
    )
    fig_fail.update_xaxes(tickformat="%d-%b")
    
else:
    # Show total failure rate
    fail_query = f"""
    SELECT d.full_date,
           COUNT(*) FILTER (WHERE f.status='failed')::float / NULLIF(COUNT(*),0) AS failure_rate
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    GROUP BY d.full_date
    ORDER BY d.full_date
    """
    
    failure_trend = pd.read_sql(fail_query, conn, params=[int(trend_start.strftime("%Y%m%d")), int(trend_end.strftime("%Y%m%d"))] + params)
    failure_trend['failure_rate'] = (
        pd.to_numeric(failure_trend['failure_rate'], errors='coerce')
          .fillna(0)
          .mul(100)
          .round(2)
          .astype(float)
    )
    failure_trend['7_day_avg'] = failure_trend['failure_rate'].rolling(7, min_periods=1).mean()
    
    fig_fail = px.line(
        failure_trend,
        x='full_date',
        y='failure_rate',
        title=f"Failure Rate Trend{filter_suffix}",
        markers=True
    )
    fig_fail.add_scatter(
        x=failure_trend['full_date'],
        y=failure_trend['7_day_avg'],
        mode='lines',
        name='7 Day Avg',
        line=dict(width=3, dash='solid'),
        hovertemplate="<b>7 Day Avg</b><br>%{x}<br>%{y:.1f}%<extra></extra>"
    )
    fig_fail.update_traces(hovertemplate="<b>%{fullData.name}</b><br>%{x}<br>%{y:.0f}%<extra></extra>")
    fig_fail.update_layout(
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121"),
        yaxis_title="Failure Rate (%)",
        xaxis_title="Date"
    )
    fig_fail.update_xaxes(tickformat="%d-%b")

st.plotly_chart(fig_fail, use_container_width=True, config={'displayModeBar': False})

# -----------------------
# Avg Processing Time Trend
# -----------------------
if breakdown_config:
    # Show top 4 breakdown + total
    proc_breakdown_query = f"""
    SELECT d.full_date, {breakdown_config['column']} as breakdown_value,
           AVG(f.processing_time) AS avg_processing_time
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    GROUP BY d.full_date, {breakdown_config['column']}
    ORDER BY d.full_date
    """
    
    proc_breakdown_df = pd.read_sql(proc_breakdown_query, conn, params=[int(trend_start.strftime("%Y%m%d")), int(trend_end.strftime("%Y%m%d"))] + params)
    
    # Get top 4 by average processing time (slowest)
    top_4_proc = proc_breakdown_df.groupby('breakdown_value')['avg_processing_time'].mean().nlargest(4).index.tolist()
    top_proc_trend = proc_breakdown_df[proc_breakdown_df['breakdown_value'].isin(top_4_proc)]
    
    # Get total avg processing time
    total_proc_query = f"""
    SELECT d.full_date,
           AVG(f.processing_time) AS avg_processing_time
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    GROUP BY d.full_date
    ORDER BY d.full_date
    """
    
    total_proc_df = pd.read_sql(total_proc_query, conn, params=[int(trend_start.strftime("%Y%m%d")), int(trend_end.strftime("%Y%m%d"))] + params)
    total_proc_df['breakdown_value'] = 'Total'
    
    combined_proc_df = pd.concat([top_proc_trend, total_proc_df], ignore_index=True)
    
    fig_proc = px.line(
        combined_proc_df,
        x='full_date',
        y='avg_processing_time',
        color='breakdown_value',
        title=f"Avg Processing Time Trend - {breakdown_config['title']}",
        markers=True
    )
    
    fig_proc.update_traces(hovertemplate="<b>%{fullData.name}</b><br>%{x}<br>%{y:.2f}s<extra></extra>")
    fig_proc.update_layout(
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121"),
        yaxis_title="Avg Processing Time (sec)",
        xaxis_title="Date",
        margin=dict(t=80, b=50, l=50, r=50),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=0.99,
            xanchor="center",
            x=0.5,
            title_text=''
        )
    )
    fig_proc.update_xaxes(tickformat="%d-%b")
    
else:
    # Show total avg processing time
    proc_query = f"""
    SELECT d.full_date,
           AVG(f.processing_time) AS avg_processing_time
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
    LEFT JOIN dim_city ci ON f.city_key = ci.city_key
    LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
    WHERE f.date_key BETWEEN %s AND %s
    {filter_clause}
    GROUP BY d.full_date
    ORDER BY d.full_date
    """
    
    proc_trend = pd.read_sql(proc_query, conn, params=[int(trend_start.strftime("%Y%m%d")), int(trend_end.strftime("%Y%m%d"))] + params)
    proc_trend['7_day_avg'] = proc_trend['avg_processing_time'].rolling(7, min_periods=1).mean()
    
    fig_proc = px.line(
        proc_trend,
        x='full_date',
        y='avg_processing_time',
        title=f"Avg Processing Time Trend{filter_suffix}",
        markers=True
    )
    fig_proc.add_scatter(
        x=proc_trend['full_date'],
        y=proc_trend['7_day_avg'],
        mode='lines',
        name='7 Day Avg',
        line=dict(width=3, dash='solid'),
        hovertemplate="<b>7 Day Avg</b><br>%{x}<br>%{y:.2f}s<extra></extra>"
    )
    fig_proc.update_traces(hovertemplate="<b>%{fullData.name}</b><br>%{x}<br>%{y:.2f}s<extra></extra>")
    fig_proc.update_layout(
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121"),
        yaxis_title="Avg Processing Time (sec)",
        xaxis_title="Date"
    )
    fig_proc.update_xaxes(tickformat="%d-%b")

st.plotly_chart(fig_proc, use_container_width=True, config={'displayModeBar': False})

# -----------------------
# Comparison Charts (Failure Rate & Avg Processing Time)
# -----------------------
# Determine what to show based on filters
active_filters = [k for k, v in filters_dict.items() if v != "All"]
num_active_filters = len(active_filters)

if num_active_filters == 0:
    # Default: Show by Channel
    # Failure Rate by Channel
    channel_fail_query = f"""
        SELECT c.channel_name,
               COUNT(*) FILTER (WHERE f.status='failed')::float / NULLIF(COUNT(*),0) * 100 AS failure_rate
        FROM fact_transactions f
        LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
        WHERE f.date_key BETWEEN %s AND %s
        GROUP BY c.channel_name
        ORDER BY failure_rate DESC
    """
    
    channel_fail = pd.read_sql(channel_fail_query, conn, params=[int(start_date.strftime("%Y%m%d")), int(end_date.strftime("%Y%m%d"))])
    
    fig_fail_comp = px.bar(
        channel_fail,
        x='channel_name',
        y='failure_rate',
        title="Failure Rate by Channel",
        text=channel_fail['failure_rate'].round(1).astype(str) + '%'
    )
    fig_fail_comp.update_traces(textposition='outside', hovertemplate="%{x}<br>%{y:.1f}%<extra></extra>", width=0.4, cliponaxis=False)
    fig_fail_comp.update_layout(
        yaxis_title="Failure Rate (%)",
        xaxis_title="Channel",
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121"),
        yaxis=dict(range=[0, channel_fail['failure_rate'].max() * 1.15])
    )
    st.plotly_chart(fig_fail_comp, use_container_width=True, config={'displayModeBar': False})
    
    # Avg Processing Time by Channel
    channel_proc_query = f"""
        SELECT c.channel_name,
               AVG(f.processing_time) AS avg_processing_time
        FROM fact_transactions f
        LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
        WHERE f.date_key BETWEEN %s AND %s
        GROUP BY c.channel_name
        ORDER BY avg_processing_time DESC
    """
    
    channel_proc = pd.read_sql(channel_proc_query, conn, params=[int(start_date.strftime("%Y%m%d")), int(end_date.strftime("%Y%m%d"))])
    
    fig_proc_comp = px.bar(
        channel_proc,
        x='channel_name',
        y='avg_processing_time',
        title="Avg Processing Time by Channel",
        text=channel_proc['avg_processing_time'].round(2).astype(str) + 's'
    )
    fig_proc_comp.update_traces(textposition='outside', hovertemplate="%{x}<br>%{y:.2f}s<extra></extra>", width=0.4, cliponaxis=False)
    fig_proc_comp.update_layout(
        yaxis_title="Avg Processing Time (sec)",
        xaxis_title="Channel",
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121"),
        yaxis=dict(range=[0, channel_proc['avg_processing_time'].max() * 1.15])
    )
    st.plotly_chart(fig_proc_comp, use_container_width=True, config={'displayModeBar': False})

elif num_active_filters == 1:
    # Single filter: Show top 6 by that dimension
    filter_key = active_filters[0]
    dimension_map = {
        "city": ("ci.city_name", "City"),
        "state": ("ci.state", "State"),
        "region": ("ci.region", "Region"),
        "channel": ("c.channel_name", "Channel"),
        "segment": ("cu.segment", "Segment")
    }
    
    col, label = dimension_map[filter_key]
    
    # Failure Rate - Top 6
    fail_query = f"""
        SELECT {col} as dimension,
               COUNT(*) FILTER (WHERE f.status='failed')::float / NULLIF(COUNT(*),0) * 100 AS failure_rate
        FROM fact_transactions f
        LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
        LEFT JOIN dim_city ci ON f.city_key = ci.city_key
        LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
        WHERE f.date_key BETWEEN %s AND %s
        GROUP BY {col}
        ORDER BY failure_rate DESC
        LIMIT 6
    """
    
    fail_data = pd.read_sql(fail_query, conn, params=[int(start_date.strftime("%Y%m%d")), int(end_date.strftime("%Y%m%d"))])
    fail_title = f"Failure Rate by Top 6 {label}s" if len(fail_data) == 6 else f"Failure Rate by {label}"
    
    fig_fail_comp = px.bar(
        fail_data,
        x='dimension',
        y='failure_rate',
        title=fail_title,
        text=fail_data['failure_rate'].round(1).astype(str) + '%'
    )
    fig_fail_comp.update_traces(textposition='outside', hovertemplate="%{x}<br>%{y:.1f}%<extra></extra>", width=0.4, cliponaxis=False)
    fig_fail_comp.update_layout(
        yaxis_title="Failure Rate (%)",
        xaxis_title=label,
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121"),
        yaxis=dict(range=[0, fail_data['failure_rate'].max() * 1.15])
    )
    st.plotly_chart(fig_fail_comp, use_container_width=True, config={'displayModeBar': False})
    
    # Avg Processing Time - Top 6
    proc_query = f"""
        SELECT {col} as dimension,
               AVG(f.processing_time) AS avg_processing_time
        FROM fact_transactions f
        LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
        LEFT JOIN dim_city ci ON f.city_key = ci.city_key
        LEFT JOIN dim_customer cu ON f.customer_key = cu.customer_key
        WHERE f.date_key BETWEEN %s AND %s
        GROUP BY {col}
        ORDER BY avg_processing_time DESC
        LIMIT 6
    """
    
    proc_data = pd.read_sql(proc_query, conn, params=[int(start_date.strftime("%Y%m%d")), int(end_date.strftime("%Y%m%d"))])
    proc_title = f"Avg Processing Time by Top 6 {label}s" if len(proc_data) == 6 else f"Avg Processing Time by {label}"
    
    fig_proc_comp = px.bar(
        proc_data,
        x='dimension',
        y='avg_processing_time',
        title=proc_title,
        text=proc_data['avg_processing_time'].round(2).astype(str) + 's'
    )
    fig_proc_comp.update_traces(textposition='outside', hovertemplate="%{x}<br>%{y:.2f}s<extra></extra>", width=0.4, cliponaxis=False)
    fig_proc_comp.update_layout(
        yaxis_title="Avg Processing Time (sec)",
        xaxis_title=label,
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121"),
        yaxis=dict(range=[0, proc_data['avg_processing_time'].max() * 1.15])
    )
    st.plotly_chart(fig_proc_comp, use_container_width=True, config={'displayModeBar': False})

else:
    # Multiple filters: Show default channel comparison
    channel_fail_query = f"""
        SELECT c.channel_name,
               COUNT(*) FILTER (WHERE f.status='failed')::float / NULLIF(COUNT(*),0) * 100 AS failure_rate
        FROM fact_transactions f
        LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
        WHERE f.date_key BETWEEN %s AND %s
        {filter_clause}
        GROUP BY c.channel_name
        ORDER BY failure_rate DESC
    """
    
    channel_fail = pd.read_sql(channel_fail_query, conn, params=[int(start_date.strftime("%Y%m%d")), int(end_date.strftime("%Y%m%d"))] + params)
    
    fig_fail_comp = px.bar(
        channel_fail,
        x='channel_name',
        y='failure_rate',
        title="Failure Rate by Channel",
        text=channel_fail['failure_rate'].round(1).astype(str) + '%'
    )
    fig_fail_comp.update_traces(textposition='outside', hovertemplate="%{x}<br>%{y:.1f}%<extra></extra>", width=0.4, cliponaxis=False)
    fig_fail_comp.update_layout(
        yaxis_title="Failure Rate (%)",
        xaxis_title="Channel",
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121"),
        yaxis=dict(range=[0, channel_fail['failure_rate'].max() * 1.15])
    )
    st.plotly_chart(fig_fail_comp, use_container_width=True, config={'displayModeBar': False})
    
    channel_proc_query = f"""
        SELECT c.channel_name,
               AVG(f.processing_time) AS avg_processing_time
        FROM fact_transactions f
        LEFT JOIN dim_channel c ON f.channel_key = c.channel_key
        WHERE f.date_key BETWEEN %s AND %s
        {filter_clause}
        GROUP BY c.channel_name
        ORDER BY avg_processing_time DESC
    """
    
    channel_proc = pd.read_sql(channel_proc_query, conn, params=[int(start_date.strftime("%Y%m%d")), int(end_date.strftime("%Y%m%d"))] + params)
    
    fig_proc_comp = px.bar(
        channel_proc,
        x='channel_name',
        y='avg_processing_time',
        title="Avg Processing Time by Channel",
        text=channel_proc['avg_processing_time'].round(2).astype(str) + 's'
    )
    fig_proc_comp.update_traces(textposition='outside', hovertemplate="%{x}<br>%{y:.2f}s<extra></extra>", width=0.4, cliponaxis=False)
    fig_proc_comp.update_layout(
        yaxis_title="Avg Processing Time (sec)",
        xaxis_title="Channel",
        plot_bgcolor='#F8F9FA',
        paper_bgcolor='#F8F9FA',
        font=dict(color="#212121"),
        yaxis=dict(range=[0, channel_proc['avg_processing_time'].max() * 1.15])
    )
    st.plotly_chart(fig_proc_comp, use_container_width=True, config={'displayModeBar': False})









