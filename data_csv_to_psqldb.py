# Description: This script creates a table in the PostgreSQL database and transfers data from .csv file to the database.

# Import the required modules
import csv
import psycopg2

# PostgreSQL database connection details
db_host: str = "localhost"
db_port: str = "5432"
db_name: str = "personinfodb"
db_user: str = "postgres"
db_password: str = "*********"

# Path to the fake_iban_names_data.csv file which is in "data" folder
mapping_csv_path: str = "data/fake_iban_names_data.csv"


def create_table(conn: psycopg2.extensions.connection) -> None:
    """
    Create the table in the PostgreSQL database.
    
    Parameters:
        conn (psycopg2.extensions.connection): The connection to the PostgreSQL database.
    
    """
    create_table_query: str = """
    CREATE TABLE IF NOT EXISTS person_info (
        full_name TEXT,
        iban TEXT PRIMARY KEY,
        email TEXT
    )
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
    conn.commit()

def transfer_data(conn: psycopg2.extensions.connection) -> None:
    """
    Transfer data from .csv to the PostgreSQL database.

    Parameters:
        conn (psycopg2.extensions.connection): The connection to the PostgreSQL database.
    
    """
    with open(mapping_csv_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        for row in reader:
            full_name: str = row["full_name"]
            iban: str = row["iban"]
            email: str = row["email"]
            insert_query: str = """
            INSERT INTO person_info (full_name, iban, email)
            VALUES (%s, %s, %s)
            """
            with conn.cursor() as cursor:
                cursor.execute(insert_query, (full_name, iban, email))
            conn.commit()

def main() -> None:
    try:
        # Establish a connection to the PostgreSQL database
        conn: psycopg2.extensions.connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )

        # Create the  table if it doesn't exist
        create_table(conn)

        # Transfer data from .csv to the database
        transfer_data(conn)

        print("Data transfer completed successfully.")
    except psycopg2.Error as e:
        print("Error transferring data:", e)
    finally:
        # Close the database connection
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
