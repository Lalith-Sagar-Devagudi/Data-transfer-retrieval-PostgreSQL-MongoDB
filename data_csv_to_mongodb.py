# Description: This script transfers data from .csv file to the MongoDB database.

# Import the required modules
import csv
from pymongo import MongoClient

# MongoDB connection details
db_host = "localhost"
db_port = 27017
db_name = "transactionsdb"

# Path to the data.csv file
data_csv_path = "data/fake_transactions_data.csv"

def transfer_data(client, csv_path):
    """
    Transfer data from the Data CSV to the MongoDB database.

    Parameters:
        client (MongoClient): A MongoClient instance connected to a MongoDB database.
        csv_path (str): The path to the CSV file containing the data to transfer.
    
    """
    db = client[db_name]
    collection = db["transactions_data"]
    batch_size = 100  # Adjust this to a value that works well for your situation
    batch = []
    with open(csv_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        for row in reader:
            date = row["date"]
            iban = row["iban"]
            amount = row["amount"]
            document = {
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

def main():
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