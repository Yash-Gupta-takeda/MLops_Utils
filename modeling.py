# Import libraries
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import pickle

# 1. Create a simple dataset
df = pd.read_csv('C:/python scripts/MLops_utils/data/training/testing.csv')

# 2. Split into features and target
X = df[['area', 'bedrooms',	'bathrooms', 'stories',	'parking']]
y = df['price']

# 3. Train-test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# 4. Train the regression model
model = LinearRegression()
model.fit(X_train, y_train)

# 5. Make predictions
y_pred = model.predict(X_test)

# 6. Evaluate the model
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)


# 7. Save the model as a pickle file
pickle_filename = "housing.pkl"
with open(pickle_filename, "wb") as file:
    pickle.dump(model, file)

print(f"\n✅ Model saved successfully as '{pickle_filename}'")

