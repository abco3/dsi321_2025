import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from prefect import flow, task, get_run_logger
import s3fs

@task
def load_data_from_lakefs(path: str, storage_options: dict) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        df = pd.read_parquet(path, storage_options=storage_options)
        if df.empty:
            logger.error("Loaded data is empty!")
            return pd.DataFrame()
        logger.info(f"Loaded data with shape {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Failed to load data from LakeFS: {e}")
        raise e

@task
def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    required_cols = ["timestamp", "nameTH", "PM25.value"]

    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        logger.error(f"Missing required columns in data: {missing_cols}")
        return pd.DataFrame()

    df = df[required_cols]
    df_filtered = df[df['PM25.value'] >= 0].copy()
    
    stations_to_remove = ['สำนักงานเขตบางคอแหลม (Mobile)', 'การเคหะชุมชนห้วยขวาง ']
    df_cleaned = df_filtered[~df_filtered['nameTH'].isin(stations_to_remove)].copy()

    logger.info(f"Preprocessed data shape: {df_cleaned.shape}")
    return df_cleaned

@task
def forecast_pm25(df_cleaned: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()

    if df_cleaned.empty:
        logger.error("Input dataframe to forecast_pm25 is empty.")
        return pd.DataFrame()

    average_pm25 = df_cleaned.groupby('nameTH')['PM25.value'].mean()
    top_5_locations = average_pm25.nlargest(5).index.tolist()
    logger.info(f"Top 5 PM2.5 locations: {top_5_locations}")

    n_predictions = 6
    min_observations = 4
    top_5_forecasts = {}
    other_forecasts = {}

    for location in df_cleaned['nameTH'].unique():
        location_data = df_cleaned[df_cleaned['nameTH'] == location].set_index('timestamp')['PM25.value'].sort_index()

        logger.info(f"Location '{location}' data points: {len(location_data)}")
        logger.info(f"Location '{location}' head data:\n{location_data.head()}")

        if len(location_data) < min_observations:
            logger.warning(f"Location '{location}' has insufficient data ({len(location_data)} points). Skipping forecast.")
            if location not in top_5_locations:
                other_forecasts[location] = [None] * n_predictions
            continue

        try:
            model = ARIMA(location_data, order=(1, 0, 1))
            model_fit = model.fit()

            forecast = model_fit.get_forecast(steps=n_predictions)
            forecast_values = forecast.predicted_mean

            if len(forecast_values) != n_predictions:
                logger.warning(f"Forecast length mismatch for location '{location}': expected {n_predictions}, got {len(forecast_values)}")
                forecast_values = forecast_values.reindex(range(n_predictions), fill_value=None)

            forecast_index = pd.date_range(start=location_data.index[-1], periods=n_predictions + 1, freq='h')[1:]
            forecast_series = pd.Series(forecast_values.values, index=forecast_index)

            if location in top_5_locations:
                top_5_forecasts[location] = forecast_series
            else:
                other_forecasts[location] = forecast_series.tolist()

        except Exception as e:
            logger.warning(f"ARIMA error for '{location}': {e}")
            if location not in top_5_locations:
                other_forecasts[location] = [None] * n_predictions

    all_forecasts_df = pd.DataFrame.from_dict({k: v.tolist() for k, v in top_5_forecasts.items()}, orient='index')
    other_forecasts_df = pd.DataFrame.from_dict(other_forecasts, orient='index')

    if not all_forecasts_df.empty:
        all_forecasts_df.columns = [f't+{i+1}hr' for i in range(n_predictions)]
    if not other_forecasts_df.empty:
        other_forecasts_df.columns = [f't+{i+1}hr' for i in range(n_predictions)]

    final_df = pd.concat([all_forecasts_df, other_forecasts_df])
    logger.info(f"Final forecast dataframe shape: {final_df.shape}")
    logger.info("Forecasting complete.")
    return final_df

@task
def save_forecast_to_lakefs(forecast_df: pd.DataFrame, path: str, storage_options: dict):
    logger = get_run_logger()
    s3 = s3fs.S3FileSystem(**storage_options)
    bucket_name = path.split('/')[2]
    file_path = '/'.join(path.split('/')[3:])

    if not s3.exists(bucket_name):
        raise FileNotFoundError(f"Bucket '{bucket_name}' not found.")
    
    if forecast_df.empty:
        logger.error("Forecast dataframe is empty. Skipping save.")
        return

    try:
        forecast_df.to_parquet(path, storage_options=storage_options)
        logger.info("Forecast data saved successfully.")
    except Exception as e:
        logger.error(f"Error saving forecast data to LakeFS: {e}")
        raise e

@flow(name="forecasting_flow")
def forecasting_flow():
    ACCESS_KEY = "access_key"
    SECRET_KEY = "secret_key"
    lakefs_endpoint = "http://lakefs-dev:8000/"
    lakefs_path = "s3a://dust-concentration/main/dust_data.parquet"
    forecast_save_path = "s3a://forecast/main/forecast-results.parquet"

    storage_options = {
        "key": ACCESS_KEY,
        "secret": SECRET_KEY,
        "client_kwargs": {"endpoint_url": lakefs_endpoint}
    }

    df = load_data_from_lakefs(lakefs_path, storage_options)
    if df.empty:
        return pd.DataFrame()  # Exit early if the data is empty

    df_cleaned = preprocess_data(df)
    forecast_df = forecast_pm25(df_cleaned)
    save_forecast_to_lakefs(forecast_df, forecast_save_path, storage_options)
    return forecast_df


if __name__ == "__main__":
    forecasting_flow()