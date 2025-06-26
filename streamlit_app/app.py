import streamlit as st
from databricks import sql
import pandas as pd
import plotly.express as px

# --- Streamlit Page Configuration ---
st.set_page_config(
    layout="wide",
    page_title="Loan Application Approval Dashboard",
    initial_sidebar_state="expanded"
)

st.title("📊 Loan Application Approval Status Overview")

st.markdown(
    """
    This dashboard provides a live overview of loan application approval statuses
    from your Databricks `test.edu_loan.applications` table.
    You can view aggregated statuses across all stages (ES, DA, OSD, MNGR, GM, MD)
    or select a specific stage for detailed analysis.
    """
)

# --- Streamlit Secrets for Databricks Connection ---
# Access credentials securely from .streamlit/secrets.toml
try:
    server_hostname = st.secrets["databricks"]["server_hostname"]
    http_path = st.secrets["databricks"]["http_path"]
    access_token = st.secrets["databricks"]["access_token"]
    default_table = st.secrets["databricks"]["table_name"] # e.g., test.edu_loan.applications
except KeyError:
    st.error(
        "Databricks credentials not found in `.streamlit/secrets.toml` or Streamlit Cloud secrets. "
        "Please check your configuration. "
        "Make sure you have `server_hostname`, `http_path`, `access_token`, and `table_name` defined under a `[databricks]` section."
    )
    st.stop() # Stop the app if credentials are missing

# --- Cached Connection to Databricks SQL Endpoint ---
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
        st.error(f"Failed to connect to Databricks: {e}")
        st.info("Please ensure your Databricks SQL Warehouse is running and credentials are correct.")
        st.stop()

# --- Function to fetch data ---
@st.cache_data(ttl=600) # Cache data for 10 minutes to reduce Databricks calls
def fetch_approval_data(table_name: str):
    """
    Fetches specific approval status data from Databricks.
    Selects only the _approval_status columns.
    """
    conn = get_databricks_connection()
    status_columns = [
        "es_approval_status", "da_approval_status", "osd_approval_status",
        "mngr_approval_status", "gm_approval_status", "md_approval_status"
    ]
    columns_str = ", ".join(status_columns)
    query = f"SELECT {columns_str} FROM {table_name} LIMIT 50000" # Added limit for safety

    try:
        with conn.cursor() as cursor:
            st.sidebar.info(f"Executing query for approval statuses...")
            cursor.execute(query)
            data = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(data, columns=column_names)
            st.sidebar.success("Data fetched successfully!")
            return df
    except Exception as e:
        st.error(f"Error executing query on Databricks: {e}")
        st.sidebar.error("Data fetch failed.")
        return pd.DataFrame() # Return empty DataFrame on error

# --- Function to process and aggregate/filter approval data ---
def process_status_data(df: pd.DataFrame, selected_stage: str = "Aggregated"):
    """
    Processes and aggregates/filters approval statuses.
    Normalizes status names (e.g., 'Approved', 'approved').
    Can aggregate all stages or focus on a specific stage.
    """
    if df.empty:
        return pd.DataFrame(), "No Data"

    status_cols = [col for col in df.columns if col.endswith('_approval_status')]

    if not status_cols:
        st.warning("No '_approval_status' columns found in the data.")
        return pd.DataFrame(), "No relevant status columns"

    # Normalize status values to lowercase and standardize
    status_mapping = {
        'approved': 'Approved',
        'accepted': 'Approved',
        'complete': 'Approved',
        'rejected': 'Rejected',
        'denied': 'Rejected',
        'in progress': 'In Progress',
        'pending': 'In Progress',
        'review': 'In Progress',
        'awaiting review': 'In Progress',
        'null': 'Unknown/Missing', # Handle potential nulls or empty strings
        'none': 'Unknown/Missing',
        '': 'Unknown/Missing'
    }

    if selected_stage == "Aggregated":
        # Melt the DataFrame to long format to easily count all statuses
        melted_df = df[status_cols].melt(var_name='approval_stage', value_name='status')
        melted_df['status'] = melted_df['status'].astype(str).str.lower().str.strip()
        melted_df['standard_status'] = melted_df['status'].map(status_mapping).fillna('Other')

        # Count occurrences of each standard status
        status_counts = melted_df['standard_status'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        chart_title = "Total Applications by Approval Status (All Stages Aggregated)"
    else:
        # Focus on a specific stage
        stage_column = f"{selected_stage.lower()}_approval_status"
        if stage_column not in df.columns:
            st.error(f"Column '{stage_column}' not found in the fetched data.")
            return pd.DataFrame(), f"Column {stage_column} not found"

        stage_df = df[[stage_column]].copy() # Use .copy() to avoid SettingWithCopyWarning
        stage_df['status'] = stage_df[stage_column].astype(str).str.lower().str.strip()
        stage_df['standard_status'] = stage_df['status'].map(status_mapping).fillna('Other')

        status_counts = stage_df['standard_status'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        chart_title = f"Applications by Approval Status for {selected_stage} Stage"

    # Sort for better visualization (e.g., Approved first, Rejected second)
    order = ['Approved', 'Rejected', 'In Progress', 'Unknown/Missing', 'Other']
    status_counts['Status'] = pd.Categorical(status_counts['Status'], categories=order, ordered=True)
    status_counts = status_counts.sort_values('Status')

    return status_counts, chart_title

# --- Main Dashboard Logic ---
with st.sidebar:
    st.header("Dashboard Controls")

    # Dropdown to select approval stage
    all_status_stages = [
        "Aggregated",
        "ES", "DA", "OSD", "MNGR", "GM", "MD"
    ]
    selected_view = st.selectbox(
        "Select Approval Stage View:",
        options=all_status_stages,
        index=0, # Default to 'Aggregated'
        help="Choose 'Aggregated' for a combined view or a specific stage for granular data."
    )

    st.write("Click below to refresh data from Databricks.")
    if st.button("Refresh Data from Databricks", help="Fetches the latest data and clears cache."):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.experimental_rerun() # Rerun the app to apply changes

    st.markdown("---")
    st.write("Current Databricks Table:")
    st.code(default_table, language="text")
    st.write("Ensure your SQL Warehouse is active for live data.")


data_load_state = st.info("Loading data from Databricks... This may take a moment.")
raw_df = fetch_approval_data(default_table)
data_load_state.empty() # Clear loading message

if not raw_df.empty:
    st.subheader(f"{selected_view} Approval Statuses Overview")

    # Process and get aggregated/filtered counts based on selection
    processed_df, chart_title_text = process_status_data(raw_df, selected_view)

    if not processed_df.empty:
        col1, col2 = st.columns([1, 2])

        with col1:
            st.markdown("#### Status Counts")
            st.dataframe(processed_df, use_container_width=True, hide_index=True)

        with col2:
            st.markdown(f"#### {chart_title_text}")
            # Create a bar chart using Plotly Express
            fig = px.bar(
                processed_df,
                x='Status',
                y='Count',
                color='Status',
                title=chart_title_text,
                labels={'Status': 'Approval Status', 'Count': 'Number of Instances'},
                template="plotly_white",
                color_discrete_map={
                    'Approved': '#28a745',       # Green
                    'Rejected': '#dc3545',       # Red
                    'In Progress': '#ffc107',    # Yellow/Orange
                    'Unknown/Missing': '#6c757d',# Grey
                    'Other': '#17a2b8'           # Info Blue
                }
            )
            fig.update_layout(showlegend=False) # Hide legend if colors are mapped to x-axis
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.subheader("Raw Data Preview (First 50 Rows)")
        st.dataframe(raw_df.head(50), use_container_width=True) # Show only first 50 rows for preview

    else:
        st.warning(f"Could not process data for '{selected_view}'. Check column names or data types.")
else:
    st.warning("No data retrieved from Databricks. Please check your connection and table name.")

st.sidebar.markdown("---")
st.sidebar.caption("Dashboard")
�