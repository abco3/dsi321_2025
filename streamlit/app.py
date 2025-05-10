import streamlit as st
import pandas as pd

ACCESS_KEY = "access_key"
SECRET_KEY = "secret_key"

lakefs_endpoint = "http://lakefs-dev:8000/"

repo = "dust-concentration"
branch = "main"
path = "dust_data.parquet"

lakefs_s3_path = f"s3a://{repo}/{branch}/{path}"

storage_options = {
    "key": ACCESS_KEY,
    "secret": SECRET_KEY,
    "client_kwargs": {
        "endpoint_url": lakefs_endpoint
    }
}

df = pd.read_parquet(
    path=lakefs_s3_path,
    storage_options=storage_options
)
st.title("Dashboard")
