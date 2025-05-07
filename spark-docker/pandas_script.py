import pandas as pd
import glob
import os

# Ruta a los archivos CSV de ventas
data_path = os.path.join(os.path.dirname(__file__), '../salesdata/Sales_April_2019.csv')

# Leer el archivo de ventas de abril
ventas = pd.read_csv(data_path)

print("--- Primeras filas del dataset ---")
print(ventas.head())

print("\n--- Estadísticas descriptivas ---")
print(ventas.describe(include='all'))

# Total de ventas
if 'Sales' in ventas.columns:
    print("\nTotal de ventas:", ventas['Sales'].sum())
    print("Media de ventas:", ventas['Sales'].mean())
    print("Máximo de ventas:", ventas['Sales'].max())
    print("Mínimo de ventas:", ventas['Sales'].min())

# Ventas por producto
if 'Product' in ventas.columns:
    print("\nVentas por producto:")
    print(ventas.groupby('Product')['Sales'].sum())

# Ventas por ciudad
if 'City' in ventas.columns:
    print("\nVentas por ciudad:")
    print(ventas.groupby('City')['Sales'].sum())
