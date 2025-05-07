\
import pandas as pd
import os
import matplotlib.pyplot as plt

# Define the path to the sales data directory
sales_data_path = "data/salesdata" # This path is relative to the script's execution context in Docker

# List all CSV files in the directory
all_files = [os.path.join(sales_data_path, f) for f in os.listdir(sales_data_path) if f.endswith('.csv')]

# Read and concatenate all CSV files
li = []
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)

frame = pd.concat(li, axis=0, ignore_index=True)

# --- Data Cleaning ---
# Drop rows with any NaN values
frame.dropna(how='any', inplace=True)

# Remove rows with 'Order ID' as header (which might appear due to concatenation issues if files have headers in data rows)
frame = frame[frame['Order ID'] != 'Order ID']

# Convert 'Quantity Ordered' and 'Price Each' to numeric
frame['Quantity Ordered'] = pd.to_numeric(frame['Quantity Ordered'])
frame['Price Each'] = pd.to_numeric(frame['Price Each'])

# Calculate 'Sales'
frame['Sales'] = frame['Quantity Ordered'] * frame['Price Each']

# Convert 'Order Date' to datetime and extract month
frame['Order Date'] = pd.to_datetime(frame['Order Date'], format='%m/%d/%y %H:%M')
frame['Month'] = frame['Order Date'].dt.month_name()


# --- Descriptive Statistics ---
print("--- Descriptive Statistics for Sales ---")
print(frame['Sales'].describe())
print("\\n")

print("--- Total Sales per Month ---")
sales_by_month = frame.groupby('Month')['Sales'].sum().sort_values(ascending=False)
print(sales_by_month)
print("\\n")

print("--- Top 5 Products by Quantity Sold ---")
top_products = frame.groupby('Product')['Quantity Ordered'].sum().sort_values(ascending=False).head(5)
print(top_products)
print("\\n")

# --- Visualization ---
# Ensure the viz directory exists
viz_dir = "data/viz" # Changed to relative path for container
if not os.path.exists(viz_dir):
    os.makedirs(viz_dir)

# 1. Histogram of Sales
plt.figure(figsize=(10, 6))
frame['Sales'].plot(kind='hist', bins=50, title='Distribution of Sales Amount')
plt.xlabel('Sales Amount')
plt.ylabel('Frequency')
plt.savefig(os.path.join(viz_dir, 'sales_distribution_histogram.png'))
plt.close()
print(f"Histogram of sales distribution saved to {os.path.join(viz_dir, 'sales_distribution_histogram.png')}")

# 2. Bar chart of Total Sales per Month
plt.figure(figsize=(12, 7))
sales_by_month_plot = frame.groupby('Month')['Sales'].sum().reindex(['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'])
sales_by_month_plot.plot(kind='bar', title='Total Sales per Month')
plt.xlabel('Month')
plt.ylabel('Total Sales ($)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(viz_dir, 'total_sales_per_month.png'))
plt.close()
print(f"Bar chart of total sales per month saved to {os.path.join(viz_dir, 'total_sales_per_month.png')}")

print("\\n--- Pandas script execution finished ---")
