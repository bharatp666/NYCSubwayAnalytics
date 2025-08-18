import pandas as pd
from google.cloud import bigquery
from xgboost import XGBRegressor, plot_importance
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

# initialize BigQuery client
client = bigquery.Client()

# Load the data from BigQuery
query = """
    SELECT *
    FROM `vigilant-armor-466416-m6.NYC_subway_ridership.date_model`
"""
df = client.query(query).to_dataframe()

# Drop the columns not used for prediction
X = df.drop(columns=["transit_date", "ridership", "transfers"])
y = df["ridership"]

# One-hot encode categorical variables
X = pd.get_dummies(X)

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y,
                                                    test_size=0.2, random_state=42)

# Train the model
model_xgb = XGBRegressor()
model_xgb.fit(X_train, y_train)

# Plot feature importance
plot_importance(model_xgb, max_num_features=25, height=0.5)
plt.tight_layout()
plt.show()