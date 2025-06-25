import streamlit as st
from databricks import sql
import pandas as pd

# Set page configuration for better layout
st.set_page_config(layout="wide", page_title="Databricks Live Dashboard")

st.title("📊 Live Dashboard from Databricks")

try:
    server_hostname = st.secrets["databricks"]["server_hostname"]
    http_path = st.secrets["databricks"]["http_path"]
    access_token = st.secrets["databricks"]["access_token"]
    default_table = st.secrets["databricks"]["table_name"]
except KeyError:
    st.error("Databricks credentials not found in `.streamlit/secrets.toml` or Streamlit Cloud secrets. Please check your configuration.")
    st.stop() 

@st.cache_resource
def get_databricks_connection():
    """Establishes and caches a connection to Databricks SQL Endpoint."""
    try:
        conn = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Databricks: {e}")
        st.stop()

@st.cache_data(ttl=600) 
def fetch_data(query: str):
    """Fetches data from Databricks using the cached connection."""
    conn = get_databricks_connection()
    try:
        with conn.cursor() as cursor:
            st.info(f"Executing query: `{query}`") 
            cursor.execute(query)
            data = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(data, columns=column_names)
            return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame() 


st.sidebar.header("Query Configuration")
query_mode = st.sidebar.radio("Select Query Mode:", ["Predefined Query", "Custom Query (Advanced)"])

sql_query = ""
if query_mode == "Predefined Query":
    st.sidebar.write(f"Querying data from: `{default_table}`")
    sql_query = f"SELECT * FROM {default_table} LIMIT 1000"
    st.sidebar.markdown(f"**Predefined Query:** `{sql_query}`")
else:
    custom_query = st.sidebar.text_area(
        "Enter your SQL Query:",
        value=f"SELECT * FROM {default_table} WHERE some_column > 100 LIMIT 500",
        height=150
    )
    if not custom_query.strip():
        st.sidebar.warning("Please enter a SQL query.")
    sql_query = custom_query

if st.sidebar.button("Refresh Data"):
    st.cache_data.clear() 
    st.success("Data cache cleared. Fetching new data...")

if sql_query:
    df = fetch_data(sql_query)

    if not df.empty:
        st.header("Raw Data Preview")
        st.dataframe(df)

        st.header("Sample Visualization")
        numeric_cols = df.columns
        if len(numeric_cols) >= 2:
            x_axis = st.selectbox("Select X-axis for chart:", numeric_cols)
            y_axis = st.selectbox("Select Y-axis for chart:", numeric_cols, index=1 if len(numeric_cols) > 1 else 0)
            if x_axis and y_axis:
                st.bar_chart(df[[x_axis, y_axis]].set_index(x_axis))
        elif len(numeric_cols) == 1:
            st.write(f"Displaying distribution of: {numeric_cols[0]}")
            st.hist_chart(df[numeric_cols[0]])
        else:
            st.info("No numeric columns found for simple charts. Add more complex visualizations based on your data.")



    else:
        st.warning("No data found for the given query or table.")
else:
    st.info("Enter a SQL query or select predefined query mode to fetch data.")

st.sidebar.markdown("---")
st.sidebar.info("Powered by Streamlit and Databricks SQL Endpoints.")