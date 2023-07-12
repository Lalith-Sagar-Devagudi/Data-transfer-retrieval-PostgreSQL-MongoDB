# Description: This script retrieves transactions from a DBs for a given person (identified by their full name) and time range.

# Load libraries
from datetime import datetime
from typing import List, Dict, Union
from pymongo import MongoClient
import pandas as pd
import psycopg2

# Define database connection details
postgres_db_host: str = "localhost"  # hostname where Postgres server is running
postgres_db_port: str = "5432"  # port number at which Postgres server is running
postgres_db_name: str = "personinfodb"  # name of the Postgres database to connect to
postgres_db_user: str = "postgres"  # username for Postgres database
postgres_db_password: str = "*******"  # password for the Postgres user

mongo_db_host: str = "localhost"  # hostname where MongoDB server is running
mongo_db_port: int = 27017  # port number at which MongoDB server is running
mongo_db_name: str = "transactionsdb"  # name of the MongoDB database to connect to


def retrieve_transactions(full_name: str, start_date: str, end_date: str) -> List[Dict[str, Union[datetime, str, float]]]:
    """
    Retrieve transactions from a MongoDB for a given person (identified by their full name) and time range.

    The person is first identified in a PostgreSQL database by their full name, from which their IBAN is retrieved.
    This IBAN is then used to query the MongoDB for all transactions in the given date range.

    Parameters:
    full_name (str): Full name of the person for whom to retrieve transaction data.
    start_date (str): Start date for the time range of transactions to retrieve, in the format 'DD/MM/YYYY'.
    end_date (str): End date for the time range of transactions to retrieve, in the format 'DD/MM/YYYY'.

    Returns:
    transactions (List[Dict[str, Union[datetime, str, float]]]): List of transactions found for the person in the given date range.
    Each transaction is represented as a dictionary with 'date', 'iban', and 'amount' as keys.
    Returns an empty list if the person's name is not found in the PostgreSQL database.
    """

    # Connect to the PostgreSQL database
    pg_conn = psycopg2.connect(
        host=postgres_db_host,
        port=postgres_db_port,
        database=postgres_db_name,
        user=postgres_db_user,
        password=postgres_db_password
    )

    # Connect to the MongoDB
    mongo_client = MongoClient(mongo_db_host, mongo_db_port)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db["transactions_data"]

    # Query PostgreSQL database for IBAN based on full_name
    pg_query = """
    SELECT iban FROM person_info WHERE full_name = %s
    """
    with pg_conn.cursor() as pg_cursor:
        pg_cursor.execute(pg_query, (full_name ,))
        result = pg_cursor.fetchone()
        if result is None:
            return []  # Name not found in database

        iban = result[0]

    # Parse start_date and end_date
    start_date = datetime.strptime(start_date, "%d/%m/%Y")
    end_date = datetime.strptime(end_date, "%d/%m/%Y")

    # Query MongoDB for transactions within the date range
    mongo_query = {
        "iban": iban,
        "date": {"$gte": start_date, "$lte": end_date}
    }
    mongo_documents = mongo_collection.find(mongo_query)

    # Prepare the list of transactions to return
    transactions = []
    for document in mongo_documents:
        transaction = {
            "date": document["date"],
            "iban": document["iban"],
            "amount": document["amount"]
        }
        transactions.append(transaction)

    return transactions


def main():
    # Ask for input
    full_name = input("Please enter the full name: ")
    start_date = input("Please enter the start date (DD/MM/YYYY): ")
    end_date = input("Please enter the end date (DD/MM/YYYY): ")

    # Retrieve transactions
    transactions = retrieve_transactions(full_name, start_date, end_date)

    # Display the information
    print("Full name:", full_name)
    df = pd.DataFrame(transactions, index=None)
    print("iban:", df['iban'].unique())
    print("--------------------------------------------------")
    print(df.drop(columns=['iban']))

    # Clean up the amount and calculate the total
    df['amount'] = df['amount'].str.replace('€', '').str.replace(',', '.').astype(float)
    print("Total amount:", round(df['amount'].sum(), 3), "€")


# call main function
if __name__ == "__main__":
    main()