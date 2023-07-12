# Description: This script transfers data from .csv file to the MongoDB database.

# Import the required modules
import csv
from pymongo import MongoClient
from typing import List, Dict, Union, Any

# MongoDB connection details
db_host: str = "localhost"
db_port: int = 27017
db_name: str = "transactionsdb"

# Path to the data.csv file
data_csv_path: str = "data/fake_transactions_data.csv"

def transfer_data(client: MongoClient, csv_path: str) -> None:
    """
    Transfer data from the Data CSV to the MongoDB database.

    Parameters:
        client (MongoClient): A MongoClient instance connected to a MongoDB database.
        csv_path (str): The path to the CSV file containing the data to transfer.
    """
    db = client[db_name]
    collection = db["transactions_data"]
    batch_size: int = 100  # Adjust this to a value that works well for your situation
    batch: List[Dict[str, Union[str, int]]] = []
    with open(csv_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        for row in reader:
            date: str = row["date"]
            iban: str = row["iban"]
            amount: str = row["amount"]
            document: Dict[str, str] = {
                "date": date,
                "iban": iban,
                "amount": amount
            }
            batch.append(document)
            if len(batch) >= batch_size:
                collection.insert_many(batch, ordered=False)  # Exclude the _id field
                batch = []
    if batch:  # Insert any remaining documents
        collection.insert_many(batch, ordered=False)  # Exclude the _id field

def main() -> None:
    client: Any = None
    try:
        # Establish a connection to the MongoDB database
        client = MongoClient(db_host, db_port)

        # Transfer data from data.csv to the database
        transfer_data(client, data_csv_path)

        print("Data transfer completed successfully.")
    except Exception as e:
        print("Error transferring data:", e)
    finally:
        # Close the database connection
        if client is not None:
            client.close()

if __name__ == "__main__":
    main()
