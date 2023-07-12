# Transactions Data Handling Using PostgreSQL and MongoDB

This repository contains a collection of Python scripts designed to handle transactions data across different databases - PostgreSQL and MongoDB. The project is composed of three primary tasks:

1. Transferring data from a .csv file to a PostgreSQL database.
2. Transferring data from a .csv file to a MongoDB database.
3. Retrieving transactions from the MongoDB database for a given person and time range, based on data in both PostgreSQL and MongoDB.

## Data

The data used in this project is dummy data generated using Mockaroo (https://www.mockaroo.com/). Mockaroo is an online tool that lets you generate custom mock data for your applications. You can specify the structure of the data, the type of data each field should contain, and generate up to thousands of rows of realistic test data.

For this project, we used Mockaroo to generate two types of data:

1. Person data `fake_iban_names_data.csv`, which includes full names, IBANs, and email addresses.
2. Transaction data `fake_transactions_data.csv`, which includes transaction dates, IBANs, and amounts.

Please note that any resemblance the mock data may have to actual persons or transactions, living or dead, is purely coincidental.

## Database Selection

### PostgreSQL

PostgreSQL is a powerful, open-source object-relational database system. It is used for handling structured data and providing support for ACID (Atomicity, Consistency, Isolation, Durability) transactions. PostgreSQL supports a number of advanced data types, like multi-dimensional arrays and user-defined types. It is great for complex, custom procedures due to its stored procedures which support multiple programming languages. PostgreSQL is typically used when complex queries, data warehousing, and performance optimization are needed. In this project, it is used for storing person data.

### MongoDB

MongoDB is a source-available cross-platform document-oriented database program. It uses JSON-like documents with optional schemas. MongoDB is developed by MongoDB Inc. and licensed under the Server Side Public License (SSPL). MongoDB is great for handling large amounts of data that can be represented in a semi-structured manner. It provides high performance, high availability, and easy scalability. In this project, it is used for storing transaction data, which could potentially be vast and complex.

## Scripts

Here's a brief description of what each script does:

1. `data_csv_to_psqldb.py`: Transfers person data from a .csv file to a PostgreSQL database.
2. `data_csv_to_mongodb.py`: Transfers transaction data from a .csv file to a MongoDB database.
3. `data_retrieval.py`: Retrieves transaction data for a given person and date range from MongoDB. The person is identified in a PostgreSQL database by their full name, from which their IBAN is retrieved. This IBAN is then used to query MongoDB for all transactions in the given date range.

## Usage

Ensure that both PostgreSQL and MongoDB are running on your machine and that the corresponding connection parameters in each script are configured properly.

To run the scripts, use Python 3.7 or above and install the required dependencies:

```
pip install -r requirements.txt
```

Then, run each script individually:

```
python data_csv_to_psqldb.py
python data_csv_to_mongodb.py
python data_retrieval.py
```