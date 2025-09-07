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
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    income FLOAT,
    name_email_similarity FLOAT,
    prev_address_months_count FLOAT,
    current_address_months_count FLOAT,
    customer_age FLOAT,
    days_since_request FLOAT,
    intended_balcon_amount FLOAT,
    payment_type TEXT,
    zip_count_4w FLOAT,
    velocity_6h FLOAT,
    velocity_24h FLOAT,
    velocity_4w FLOAT,
    bank_branch_count_8w FLOAT,
    date_of_birth_distinct_emails_4w FLOAT,
    employment_status TEXT,
    credit_risk_score FLOAT,
    email_is_free FLOAT,
    housing_status TEXT,
    phone_home_valid FLOAT,
    phone_mobile_valid FLOAT,
    bank_months_count FLOAT,
    has_other_cards FLOAT,
    proposed_credit_limit FLOAT,
    foreign_request FLOAT,
    source TEXT,
    session_length_in_minutes FLOAT,
    device_os TEXT,
    keep_alive_session FLOAT,
    device_distinct_emails_8w FLOAT,
    device_fraud_count FLOAT,
    month FLOAT,
    score FLOAT,
    probability FLOAT
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
