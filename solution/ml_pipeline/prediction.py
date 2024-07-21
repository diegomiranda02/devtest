# prediction.py

import pandas as pd
from feast import FeatureStore
import joblib
from datetime import datetime, timezone

# Load the trained model
model = joblib.load("models/random_forest_model.joblib")

# Initialize the Feast feature store
fs = FeatureStore(repo_path=".")

# Define the provided timestamps for prediction
timestamps = [
    datetime(2024, 7, 21, 20, 9, 30, tzinfo=timezone.utc)
]

# Define the entity key and use the provided timestamps
entity_keys = [0]

# Include the event_timestamp column in the entity DataFrame
entity_df = pd.DataFrame({
    "source_ip": entity_keys,
    "event_timestamp": timestamps
})

print("Entity DataFrame for prediction:")
print(entity_df)

# Define the features to retrieve
features = [
    "combined_features:floor_state",
    "combined_features:vacant",
    "combined_features:second",
    "combined_features:minute",
    "combined_features:hour",
    "combined_features:day",
    "combined_features:day_of_week",
    "combined_features:month",
    "combined_features:year",
    "combined_features:is_working_day",
    "combined_features:is_holiday",
]

# Retrieve online feature values for prediction
online_features = fs.get_online_features(
    features=features,
    entity_rows=entity_df[["source_ip"]].to_dict('records')
).to_df()

# Add the event_timestamp back to the features
online_features["event_timestamp"] = entity_df["event_timestamp"]

print("Online Feature DataFrame for prediction:")
print(online_features)

# Handle NaN values: fill with 0 or any appropriate value
online_features.fillna(0, inplace=True)

print("Online Feature DataFrame after filling NaNs:")
print(online_features)

# Ensure the feature columns are numeric
for col in online_features.columns:
    online_features[col] = pd.to_numeric(online_features[col], errors='ignore')

# Make predictions
predictions = model.predict(online_features)

# Output the predictions
print("Predictions:")
print(predictions)
