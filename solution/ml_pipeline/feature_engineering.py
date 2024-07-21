import pandas as pd
from sqlalchemy import create_engine
from workalendar.usa import UnitedStates
import os
from feast import FeatureStore, FeatureView, Entity, FileSource, Field, ValueType
from feast.types import Int64, Bool
from datetime import timedelta, datetime
from feast.infra.offline_stores.file_source import ParquetFormat

# Set the database URL to a folder ../database in the project
DATABASE_URL = "sqlite:///../database/elevator.db"

# Ensure the directory exists
os.makedirs(os.path.dirname(DATABASE_URL.replace("sqlite:///", "")), exist_ok=True)

# Establish a connection to the database
engine = create_engine(DATABASE_URL)

# Initialize the calendar for holidays and working days
cal = UnitedStates()

def extract_time_features(df, timestamp_col):
    df['second'] = df[timestamp_col].dt.second
    df['minute'] = df[timestamp_col].dt.minute
    df['hour'] = df[timestamp_col].dt.hour
    df['day'] = df[timestamp_col].dt.day
    df['day_of_week'] = df[timestamp_col].dt.dayofweek
    df['month'] = df[timestamp_col].dt.month
    df['year'] = df[timestamp_col].dt.year
    df['is_working_day'] = df[timestamp_col].apply(lambda x: cal.is_working_day(x))
    df['is_holiday'] = df[timestamp_col].apply(lambda x: cal.is_holiday(x))
    return df

# Read data from the 'elevator_demand' table
query_demand = "SELECT * FROM elevator_demand"
df_demand = pd.read_sql(query_demand, engine)
df_demand['timestamp'] = pd.to_datetime(df_demand['timestamp'])
df_demand = df_demand.ffill().set_index('timestamp').resample('10s').ffill().reset_index()

# Debug print
print("After resampling 'elevator_demand' data:")
print(df_demand.head())

# Read data from the 'elevator_state' table
query_state = "SELECT * FROM elevator_state"
df_state = pd.read_sql(query_state, engine)
df_state['timestamp'] = pd.to_datetime(df_state['timestamp'])
df_state = df_state.ffill().set_index('timestamp').resample('10s').ffill().reset_index()

# Debug print
print("After resampling 'elevator_state' data:")
print(df_state.head())

# Create unique keys for merging by combining 'timestamp' and 'source_ip'
df_demand['merge_key'] = df_demand['timestamp'].astype(str) + '_' + df_demand['source_ip']
df_state['merge_key'] = df_state['timestamp'].astype(str) + '_' + df_state['source_ip']

# Merge the two dataframes on the 'merge_key' column
df_combined = pd.merge(df_demand, df_state, on='merge_key', suffixes=('_demand', '_state'))
df_combined.dropna(inplace=True)

# Extract time features
df_combined = extract_time_features(df_combined, 'timestamp_demand')

# Convert IP addresses to categorical values
df_combined['source_ip'] = df_combined['source_ip_demand'].astype('category').cat.codes

# Drop the merge_key and timestamp_state columns
df_combined.drop(columns=['merge_key', 'source_ip_demand', 'source_ip_state', 'timestamp_state'], inplace=True)

# Rename timestamp_demand to timestamp
df_combined.rename(columns={'timestamp_demand': 'timestamp'}, inplace=True)

print(df_combined.head())

# Ensure the 'data' directory exists
os.makedirs('data', exist_ok=True)

# Save the feature engineered data to a Parquet file
parquet_path = 'data/feature_engineered_data.parquet'
df_combined.to_parquet(parquet_path, index=False)

# Define Feast entities, feature views, and ingest data
source_ip = Entity(name="source_ip", value_type=ValueType.INT64)

parquet_file_source = FileSource(
    file_format=ParquetFormat(),
    path=f"file://{os.path.abspath(parquet_path)}",
    event_timestamp_column="timestamp",  # Use the renamed timestamp column
    created_timestamp_column=""
)

combined_features = FeatureView(
    name="combined_features",
    entities=[source_ip],
    ttl=timedelta(minutes=10),  # Adjusted TTL to 10 minutes
    schema=[
        Field(name="floor_demand", dtype=Int64),
        Field(name="floor_state", dtype=Int64),
        Field(name="vacant", dtype=Bool),
        Field(name="second", dtype=Int64),
        Field(name="minute", dtype=Int64),
        Field(name="hour", dtype=Int64),
        Field(name="day", dtype=Int64),
        Field(name="day_of_week", dtype=Int64),
        Field(name="month", dtype=Int64),
        Field(name="year", dtype=Int64),
        Field(name="is_working_day", dtype=Bool),
        Field(name="is_holiday", dtype=Bool),
    ],
    online=True,
    source=parquet_file_source,
    tags={},
)

# Apply the feature views and ingest the data into the Feast feature store
fs = FeatureStore(repo_path=".")
fs.apply([source_ip, combined_features])
fs.materialize_incremental(end_date=datetime.now())

print("Feature engineering and ingestion completed.")
