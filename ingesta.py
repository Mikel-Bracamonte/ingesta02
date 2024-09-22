import boto3
import pandas as pd
import mysql.connector

def read_mysql_to_csv(host, port, user, password, database, table, csv_file):
    # Conectar a la base de datos MySQL
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )

    # Leer la tabla en un DataFrame de pandas
    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, conn)

    # Guardar el DataFrame en un archivo CSV
    df.to_csv(csv_file, index=False)

    # Cerrar la conexión
    conn.close()

    print(f'Tabla {table} guardada en {csv_file} exitosamente.')

host = '50.16.27.25'
port = '8005'
user = 'root'
password = 'utec'
database = 'tienda'
table = 'fabricantes'
csv_file = 'data.csv'

read_mysql_to_csv(host, port, user, password, database, table, csv_file)

nombreBucket = "bucket-mysql-output"

s3 = boto3.client('s3')
response = s3.upload_file(csv_file, nombreBucket, csv_file)

print("Ingesta completada")
