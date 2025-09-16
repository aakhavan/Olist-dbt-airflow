import streamlit as st
import pandas as pd
import clickhouse_connect
import os
from dotenv import load_dotenv


load_dotenv()

st.set_page_config(
    page_title="Olist Product Performance",
    layout="wide",
)

@st.cache_resource
def get_clickhouse_client():
    try:
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            database='reporting'
        )
        return client
    except Exception as e:
        st.error(f"Failed to connect to ClickHouse: {e}")
        return None

client = get_clickhouse_client()

@st.cache_data(ttl=600)
def load_performance_data():
    if client:
        try:
            df = client.query_df("""
                SELECT
                    product_id,
                    order_date,
                    product_category,
                    total_units_sold,
                    total_revenue,
                    average_review_score
                FROM mart_product_performance
            """)
            if 'order_date' in df.columns:
                df['order_date'] = pd.to_datetime(df['order_date'])
            return df
        except Exception as e:
            st.error(f"Failed to query data from mart_product_performance: {e}")
    return pd.DataFrame()

df_perf = load_performance_data()

st.title("Olist Product Performance Dashboard")

if not df_perf.empty and 'order_date' in df_perf.columns and 'product_category' in df_perf.columns:
    st.header("Monthly Units Sold Trend for Top 5 Product Categories")
    st.write("Comparing the monthly units sold for the top 5 most popular product categories.")

    top_5_categories = df_perf.groupby('product_category')['total_units_sold'].sum().nlargest(5).index
    df_top5 = df_perf[df_perf['product_category'].isin(top_5_categories)]

    df_monthly = df_top5.groupby([df_top5['order_date'].dt.to_period('M'), 'product_category'])['total_units_sold'].sum().unstack()
    df_monthly.index = df_monthly.index.to_timestamp()

    st.line_chart(df_monthly)

    # --- Restoring the Review Score vs Units Sold chart ---
    st.header("Product Review Score vs. Units Sold")
    st.write("Scatter plot showing the relationship between average review score and total units sold for each product.")

    # Aggregate data at the product level for this chart
    product_agg_df = df_perf.groupby('product_id').agg(
        total_units_sold=('total_units_sold', 'sum'),
        average_review_score=('average_review_score', 'mean'),
        total_revenue=('total_revenue', 'sum')
    ).reset_index()
    
    # Filter out zero values for a cleaner plot
    scatter_data = product_agg_df[(product_agg_df['average_review_score'] > 0) & (product_agg_df['total_units_sold'] > 0)]

    st.scatter_chart(
        scatter_data,
        x='average_review_score',
        y='total_units_sold',
        size='total_revenue'
    )

    st.header("Detailed Product Performance Data")
    st.dataframe(df_perf)

    st.sidebar.header("About")
    st.sidebar.info(
        "This dashboard visualizes product performance data from the Olist dataset, "
        "processed by an ELT pipeline using Airflow, dbt, and ClickHouse."
    )
else:
    st.warning("No data found or required columns are missing. Please ensure the dbt model has run successfully and then clear the Streamlit cache.")
