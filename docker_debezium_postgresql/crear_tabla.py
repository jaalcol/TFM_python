import psycopg2
from psycopg2 import Error

# Parámetros de conexión
db_host = "postgres"  # La dirección IP del contenedor de PostgreSQL
db_name = "quotes_API"  # El nombre de la base de datos
db_user = "admin"  # El nombre de usuario de PostgreSQL
db_password = "admin"  # La contraseña de PostgreSQL
db_port = "5432"

# Sentencia SQL para crear la tabla
create_tables_query = '''
CREATE TABLE IF NOT EXISTS quotes_price (
    id SERIAL PRIMARY KEY,
    activity TEXT UNIQUE,
    type TEXT,
    price TEXT
);
CREATE TABLE IF NOT EXISTS quotes_accesibility (
    id SERIAL PRIMARY KEY,
    activity TEXT UNIQUE,
    link TEXT,
    key TEXT,
    accessibility float
);
'''

try:
    # Establecer la conexión con la base de datos
    connection = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )

    # Crear un cursor para ejecutar comandos SQL
    cursor = connection.cursor()

    # Ejecutar la sentencia para crear la tabla
    cursor.execute(create_tables_query)
    
    # Confirmar los cambios
    connection.commit()
    
    print("Tabla creada exitosamente.")

except (Exception, Error) as error:
    print("Error al conectarse a PostgreSQL:", error)

finally:
    # Cerrar el cursor y la conexión
    if connection:
        cursor.close()
        connection.close()
        print("Conexión cerrada.")
