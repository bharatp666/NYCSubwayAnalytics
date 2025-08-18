import pandas as pd
from google.cloud import bigquery
from xgboost import XGBRegressor, plot_importance
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import OrdinalEncoder
import matplotlib.pyplot as plt

# Initialize BigQuery client
client = bigquery.Client()

# Load the data from BigQuery
query = """
SELECT *
FROM `vigilant-armor-466416-m6.NYC_subway_ridership.date_model`
LIMIT 50000
"""
df = client.query(query).to_dataframe()

# Drop non-feature and target columns
X = df.drop(columns=["transit_date", "ridership", "transfers", "station_complex"])
y = df["ridership"]

# Identify categorical columns
categorical_cols = X.select_dtypes(include=["object", "category"]).columns

# Apply ordinal encoding
encoder = OrdinalEncoder()
X[categorical_cols] = encoder.fit_transform(X[categorical_cols])

# Split data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Train model
model_xgb = XGBRegressor()
model_xgb.fit(X_train, y_train)

# Evaluate
y_pred = model_xgb.predict(X_test)
mae = mean_absolute_error(y_test, y_pred)
print(f"Mean Absolute Error: {mae:.2f}")

# Plot feature importance
plot_importance(model_xgb, max_num_features=25, height=0.5)
plt.title("Feature importance")
plt.tight_layout()
plt.show()