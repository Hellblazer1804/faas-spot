import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# Read the CSV file into a DataFrame
df = pd.read_csv("merged_function_invocation.csv")

# Filter rows where 'Count' is a valid integer
valid_rows = df['Count'].str.isnumeric()
df = df[valid_rows]

# Convert the 'Count' column to integers
df['Count'] = df['Count'].astype(int)

# Min-max scaling for the 'Count' column
scaler = MinMaxScaler(feature_range=(60, 3600))
df['Count_Scaled'] = scaler.fit_transform(df[['Count']])

# Save the 'Count' column and scaled 'Count' column to a new CSV file
df[['Count', 'Count_Scaled']].to_csv('count_column_scaled.csv', index=False)
