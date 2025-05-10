import streamlit as st
import pandas as pd

ACCESS_KEY = "access_key"
SECRET_KEY = "secret_key"

lakefs_endpoint = "http://lakefs-dev:8000/"

repo = "weather2"
branch = "main"
path = "weather2.parquet"

lakefs_s3_path = f"s3a://{repo}/{branch}/{path}"

storage_options = {
    "key": ACCESS_KEY,
    "secret": SECRET_KEY,
    "client_kwargs": {
        "endpoint_url": lakefs_endpoint
    }
}

df2 = pd.read_parquet(
    path=lakefs_s3_path,
    storage_options=storage_options
)

st.title("Weather Dashboard")

st.write("Weather Metrics", df2)

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Temperature (°C)", f"{df2['Temperature'].mean():.2f} °C")
with col2:
    st.metric("Humidity (%)", f"{df2['Humidity'].mean():.2f} %")
with col3:
    st.metric("Wind Speed (km/h)", f"{df2['Wind Speed'].mean():.2f} km/h")

st.subheader("Temperature Trend")
time_series_data = {
    'Time': pd.date_range(start="2023-04-01", periods=10, freq='D'),
    'Temperature': df2['Temperature'].sample(10).values, 
}

temp_df = pd.DataFrame(time_series_data)

st.line_chart(temp_df.set_index('Time'))

if st.button('Refresh Now'):
    st.experimental_rerun()