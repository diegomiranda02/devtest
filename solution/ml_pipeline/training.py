import pandas as pd
from feast import FeatureStore
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import joblib
from datetime import datetime, timezone

# Initialize the Feast feature store
fs = FeatureStore(repo_path=".")

# Define the provided timestamps
timestamps = [
    datetime(2024, 7, 21, 20, 9, 30, tzinfo=timezone.utc),
    datetime(2024, 7, 21, 20, 9, 40, tzinfo=timezone.utc),
    datetime(2024, 7, 21, 20, 9, 50, tzinfo=timezone.utc)
]

# Define the entity key and use the provided timestamps
entity_keys = [0, 0, 0]

entity_df = pd.DataFrame({
    "source_ip": entity_keys,
    "event_timestamp": timestamps
})

print("Entity DataFrame:")
print(entity_df)

# Define the features to retrieve
features = [
    "combined_features:floor_demand",
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

# Retrieve historical feature values for training
training_df = fs.get_historical_features(
    entity_df=entity_df,
    features=features,
    full_feature_names=False,
).to_df()

print("Training DataFrame after retrieval:")
print(training_df.head())

# Convert datetime columns to numerical format
training_df['event_timestamp'] = pd.to_numeric(training_df['event_timestamp'])

# Handle NaN values: drop rows where 'floor_demand' or any feature is NaN
training_df.dropna(subset=["floor_demand"], inplace=True)

print("Training DataFrame after dropping NaNs:")
print(training_df.head())

# Preprocess the data
# Assuming 'floor_demand' is the target variable
X = training_df.drop(columns=["floor_demand"])
y = training_df["floor_demand"]

# Ensure the target variable is numeric
y = pd.to_numeric(y)

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train a RandomForestRegressor
clf = RandomForestRegressor(n_estimators=100, random_state=42)
clf.fit(X_train, y_train)

# Evaluate the model
y_pred = clf.predict(X_test)
print("Mean Squared Error:", mean_squared_error(y_test, y_pred))

# Save the trained model
joblib.dump(clf, "models/random_forest_model.joblib")

print("Model training completed and saved to models/random_forest_model.joblib.")
