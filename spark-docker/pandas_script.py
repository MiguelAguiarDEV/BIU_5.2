import pandas as pd
import glob
import os
import matplotlib.pyplot as plt

# Ruta a los archivos CSV de ventas
data_path = os.path.join(os.path.dirname(__file__), 'salesdata/Sales_April_2019.csv')

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

# Crear carpeta viz si no existe
viz_path = os.path.join(os.path.dirname(__file__), 'viz')
os.makedirs(viz_path, exist_ok=True)

# Histograma de ventas
if 'Sales' in ventas.columns:
    plt.figure(figsize=(8, 5))
    ventas['Sales'].hist(bins=30)
    plt.title('Histograma de Ventas')
    plt.xlabel('Ventas')
    plt.ylabel('Frecuencia')
    plt.tight_layout()
    plt.savefig(os.path.join(viz_path, 'histograma_ventas.png'))
    plt.close()

# Gráfico de barras de ventas por producto
if 'Product' in ventas.columns and 'Sales' in ventas.columns:
    ventas_prod = ventas.groupby('Product')['Sales'].sum().sort_values()
    plt.figure(figsize=(10, 6))
    ventas_prod.plot(kind='barh')
    plt.title('Ventas por Producto')
    plt.xlabel('Ventas')
    plt.ylabel('Producto')
    plt.tight_layout()
    plt.savefig(os.path.join(viz_path, 'ventas_por_producto.png'))
    plt.close()

# Gráfico de barras de ventas por ciudad
if 'City' in ventas.columns and 'Sales' in ventas.columns:
    ventas_city = ventas.groupby('City')['Sales'].sum().sort_values()
    plt.figure(figsize=(10, 6))
    ventas_city.plot(kind='barh', color='orange')
    plt.title('Ventas por Ciudad')
    plt.xlabel('Ventas')
    plt.ylabel('Ciudad')
    plt.tight_layout()
    plt.savefig(os.path.join(viz_path, 'ventas_por_ciudad.png'))
    plt.close()
