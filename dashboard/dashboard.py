import streamlit as st
import pandas as pd
import clickhouse_connect
import os

# --- Page Config ---
st.set_page_config(
    page_title="Olist Product Performance",
    page_icon="🚀",
    layout="wide",
)

# --- Database Connection ---
@st.cache_resource
def get_clickhouse_client():
    """Establishes a cached connection to the ClickHouse database."""
    try:
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse-server"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            user=os.getenv("CLICKHOUSE_USER", "admin"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "admin"),
            database='reporting'
        )
        return client
    except Exception as e:
        st.error(f"Failed to connect to ClickHouse: {e}")
        return None

client = get_clickhouse_client()

# --- Data Loading ---
@st.cache_data(ttl=600)
def load_performance_data():
    """Loads product performance data from the mart table."""
    if client:
        try:
            df = client.query_df("SELECT * FROM mart_product_performance")
            return df
        except Exception as e:
            st.error(f"Failed to query data from mart_product_performance: {e}")
    return pd.DataFrame()

df_perf = load_performance_data()

# --- Dashboard UI ---
st.title("🚀 Olist Product Performance Dashboard")

if not df_perf.empty:
    st.header("Top 5 Best-Selling Product Categories")
    st.write("Ranking product categories by the total number of units sold.")
    top_categories = df_perf.groupby('product_category')['total_units_sold'].sum().nlargest(5)
    st.bar_chart(top_categories)

    st.header("Review Score vs. Units Sold")
    st.write("Analyzing the relationship between a product's average review score and the total number of units sold.")
    scatter_df = df_perf[(df_perf['average_review_score'] > 0) & (df_perf['total_units_sold'] > 0)]
    st.scatter_chart(scatter_df, x='average_review_score', y='total_units_sold', size='total_revenue')

    st.header("Detailed Product Performance Data")
    st.dataframe(df_perf)

    st.sidebar.header("About")
    st.sidebar.info(
        "This dashboard visualizes product performance data from the Olist dataset, "
        "processed by an ELT pipeline using Airflow, dbt, and ClickHouse."
    )

else:
    st.warning("No data found or failed to connect to the database. Please ensure the ELT pipeline has run successfully.")