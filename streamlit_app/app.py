import streamlit as st
from databricks import sql
import pandas as pd
import plotly.express as px
from datetime import datetime

# --- Streamlit Page Configuration ---
st.set_page_config(
    layout="wide",
    page_title="Unified Dashboard",
    initial_sidebar_state="expanded"
)

st.title("Unified Dashboard")

# --- Streamlit Secrets for Databricks Connection ---
# Access credentials securely from .streamlit/secrets.toml
try:
    server_hostname = st.secrets["databricks"]["server_hostname"]
    http_path = st.secrets["databricks"]["http_path"]
    access_token = st.secrets["databricks"]["access_token"]
    default_table = st.secrets["databricks"]["table_name"] # e.g., test.edu_loan.applications
    religion_table = st.secrets["databricks"].get("religion_table", "test.edu_loan.religions") # Assuming a religion lookup table
    district_table = st.secrets["databricks"].get("district_table", "test.edu_loan.districts") # Assuming a district lookup table
except KeyError as e:
    st.error(
        f"Databricks credentials or table names not found in `.streamlit/secrets.toml` or Streamlit Cloud secrets. "
        f"Missing key: {e}. Please check your configuration. "
        "Make sure you have `server_hostname`, `http_path`, `access_token`, `table_name`, "
        "`religion_table`, and `district_table` defined under a `[databricks]` section."
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

# --- Function to fetch main application data ---
@st.cache_data(ttl=600) # Cache data for 10 minutes to reduce Databricks calls
def fetch_application_data(table_name: str):
    """
    Fetches application data including approval statuses, gender, dob, religion_id, and present_district_id.
    """
    conn = get_databricks_connection()
    columns_to_fetch = [
        "es_approval_status", "da_approval_status", "osd_approval_status",
        "mngr_approval_status", "gm_approval_status", "md_approval_status",
        "gender", "dob", "religion_id", "present_district_id"
    ]
    columns_str = ", ".join(columns_to_fetch)
    query = f"SELECT {columns_str} FROM {table_name} LIMIT 50000" # Added limit for safety

    try:
        with conn.cursor() as cursor:
            st.sidebar.info(f"Executing query for application data...")
            cursor.execute(query)
            data = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(data, columns=column_names)
            st.sidebar.success("Application data fetched successfully!")
            return df
    except Exception as e:
        st.error(f"Error executing query on Databricks for application data: {e}")
        st.sidebar.error("Application data fetch failed.")
        return pd.DataFrame() # Return empty DataFrame on error

# --- Function to fetch religion lookup data ---
@st.cache_data(ttl=3600) # Cache religion data for 1 hour
def fetch_religions(table_name: str):
    """Fetches religion lookup data (id, religion_name)."""
    conn = get_databricks_connection()
    query = f"SELECT id, religion_name FROM {table_name}"
    try:
        with conn.cursor() as cursor:
            st.sidebar.info("Fetching religion data...")
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['id', 'religion_name'])
            st.sidebar.success("Religion data fetched!")
            return df
    except Exception as e:
        st.error(f"Error fetching religion data from {table_name}: {e}")
        st.sidebar.error("Religion data fetch failed.")
        return pd.DataFrame()

# --- Function to fetch district lookup data ---
@st.cache_data(ttl=3600) # Cache district data for 1 hour
def fetch_districts(table_name: str):
    """Fetches district lookup data (id, district_name)."""
    conn = get_databricks_connection()
    query = f"SELECT id, district_name FROM {table_name}"
    try:
        with conn.cursor() as cursor:
            st.sidebar.info("Fetching district data...")
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['id', 'district_name'])
            st.sidebar.success("District data fetched!")
            return df
    except Exception as e:
        st.error(f"Error fetching district data from {table_name}: {e}")
        st.sidebar.error("District data fetch failed.")
        return pd.DataFrame()

# --- Function to process and aggregate/filter approval data ---
def process_status_data(df: pd.DataFrame, selected_stage: str = "Aggregated", status_filter: list = None):
    """
    Processes and aggregates/filters approval statuses.
    Normalizes status names (e.g., 'Approved', 'approved').
    Can aggregate all stages or focus on a specific stage.
    Applies an optional filter for specific status types.
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

        # Apply status filter if provided
        if status_filter:
            melted_df = melted_df[melted_df['standard_status'].isin(status_filter)]

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

        # Apply status filter if provided
        if status_filter:
            stage_df = stage_df[stage_df['standard_status'].isin(status_filter)]

        status_counts = stage_df['standard_status'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        chart_title = f"Applications by Approval Status for {selected_stage} Stage"

    # Sort for better visualization (e.g., Approved first, Rejected second)
    order = ['Approved', 'Rejected', 'In Progress', 'Unknown/Missing', 'Other']
    # Filter order based on what's actually present in data and selected filter
    relevant_order = [s for s in order if s in status_counts['Status'].unique()]
    if status_filter:
        relevant_order = [s for s in relevant_order if s in status_filter]

    status_counts['Status'] = pd.Categorical(status_counts['Status'], categories=relevant_order, ordered=True)
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

    # Multiselect for filtering by specific status types
    all_status_types = ['Approved', 'Rejected', 'In Progress', 'Unknown/Missing', 'Other']
    selected_status_types = st.multiselect(
        "Filter by Result Status:",
        options=all_status_types,
        default=all_status_types, # Default to all selected
        help="Select specific types of statuses to include in the charts."
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
raw_df = fetch_application_data(default_table)
religions_df = fetch_religions(religion_table)
districts_df = fetch_districts(district_table)
data_load_state.empty() # Clear loading message

if not raw_df.empty:
    # --- Approval Status Charts ---
    st.subheader(f"{selected_view} Approval Statuses Overview")

    # Process and get aggregated/filtered counts based on selection, applying new status filter
    processed_df, chart_title_text = process_status_data(raw_df, selected_view, selected_status_types)

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

    else:
        st.warning(f"No data to display for approval statuses after filtering for '{selected_view}' with selected status types. "
                   "Try adjusting your 'Filter by Result Status' selection.")

    st.markdown("---") # Separator for new charts

    # --- New Charts Section ---
    st.subheader("Applicant Demographics and Location")

    # Chart 1: Gender Distribution
    if 'gender' in raw_df.columns and not raw_df['gender'].empty:
        gender_counts = raw_df['gender'].value_counts().reset_index()
        gender_counts.columns = ['Gender', 'Count']
        if not gender_counts.empty:
            fig_gender = px.pie(
                gender_counts,
                values='Count',
                names='Gender',
                title='Number of Applicants by Gender',
                template="plotly_white"
            )
            st.plotly_chart(fig_gender, use_container_width=True)
        else:
            st.info("No gender data available or all values are null/empty.")
    else:
        st.info("Gender column not found or is empty in the dataset.")


    # Chart 2: Age Distribution
    if 'dob' in raw_df.columns and not raw_df['dob'].empty:
        # Calculate age from DOB
        raw_df['dob'] = pd.to_datetime(raw_df['dob'], errors='coerce')
        raw_df['age'] = raw_df['dob'].apply(lambda x: (datetime.now().year - x.year - ((datetime.now().month, datetime.now().day) < (x.month, x.day))) if pd.notna(x) else None)
        raw_df['age'] = raw_df['age'].fillna(-1).astype(int) # Use -1 for unknown age

        # Create age bins for histogram
        bins = list(range(0, 101, 10)) # 0-10, 10-20, ..., 90-100
        age_range_labels = [f'{i}-{i+9}' for i in bins[:-1]]
        age_range_labels.append('100+') # For ages 100 and above
        
        # Ensure the order of age groups for the chart
        age_group_order = ['Unknown'] + age_range_labels
        
        # Binning function with handling for -1 (unknown)
        def age_bin_label(age):
            if age == -1:
                return 'Unknown'
            for i in range(len(bins) - 1):
                if bins[i] <= age < bins[i+1]:
                    return age_range_labels[i] # Correct index for age ranges
            if age >= bins[-1]:
                return '100+'
            return 'Unknown' # Fallback for unexpected values
            
        raw_df['age_group'] = raw_df['age'].apply(age_bin_label)
        
        age_counts = raw_df['age_group'].value_counts().reset_index()
        age_counts.columns = ['Age Group', 'Count']
        age_counts['Age Group'] = pd.Categorical(age_counts['Age Group'], categories=age_group_order, ordered=True)
        age_counts = age_counts.sort_values('Age Group')


        if not age_counts.empty:
            fig_age = px.bar(
                age_counts,
                x='Age Group',
                y='Count',
                title='Number of Applicants by Age Group',
                template="plotly_white",
                color='Age Group'
            )
            st.plotly_chart(fig_age, use_container_width=True)
        else:
            st.info("No age data available or all values are null/empty.")
    else:
        st.info("Date of Birth (dob) column not found or is empty in the dataset.")


    # Chart 3: Religion Distribution
    if 'religion_id' in raw_df.columns and not raw_df['religion_id'].empty and not religions_df.empty:
        # Merge with religion lookup table to get names
        religion_data = raw_df.merge(religions_df, left_on='religion_id', right_on='id', how='left')
        religion_data['religion_name'] = religion_data['religion_name'].fillna('Unknown Religion')
        
        religion_counts = religion_data['religion_name'].value_counts().reset_index()
        religion_counts.columns = ['Religion', 'Count']
        if not religion_counts.empty:
            fig_religion = px.bar(
                religion_counts,
                x='Religion',
                y='Count',
                title='Number of Applicants by Religion',
                template="plotly_white",
                color='Religion'
            )
            st.plotly_chart(fig_religion, use_container_width=True)
        else:
            st.info("No religion data available or all values are null/empty after joining.")
    else:
        st.info("Religion ID column not found, is empty in the dataset, or religion lookup data is missing.")

    # Chart 4: District Distribution
    if 'present_district_id' in raw_df.columns and not raw_df['present_district_id'].empty and not districts_df.empty:
        # Merge with district lookup table to get names
        district_data = raw_df.merge(districts_df, left_on='present_district_id', right_on='id', how='left')
        district_data['district_name'] = district_data['district_name'].fillna('Unknown District')

        district_counts = district_data['district_name'].value_counts().reset_index()
        district_counts.columns = ['District', 'Count']
        if not district_counts.empty:
            fig_district = px.bar(
                district_counts,
                x='District',
                y='Count',
                title='Number of Applicants by District',
                template="plotly_white",
                color='District'
            )
            st.plotly_chart(fig_district, use_container_width=True)
        else:
            st.info("No district data available or all values are null/empty after joining.")
    else:
        st.info("Present District ID column not found, is empty in the dataset, or district lookup data is missing.")

else:
    st.warning("No data retrieved from Databricks. Please check your connection and table name.")

st.sidebar.markdown("---")
st.sidebar.caption("Dashboard")
