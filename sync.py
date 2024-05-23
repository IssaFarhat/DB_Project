import time
import pandas as pd
import mysql.connector

# Database connection details
db_config = {
    'host': 'localhost',
    'user': 'root',        # Replace with your MySQL username
    'password': '',    # Replace with your MySQL password
    'database': 'library-db'     # Replace with your MySQL database name
}

# SQL commands to create tables
create_tables_sql = """
CREATE TABLE IF NOT EXISTS Members (
    MemberID INT PRIMARY KEY,
    MemberName VARCHAR(255),
    MemberLastDateSubscribed DATE
);

CREATE TABLE IF NOT EXISTS Employees (
    EmployeeID INT PRIMARY KEY,
    EmployeeName VARCHAR(255),
    EmployeeEmployedDate DATE,
    EmployeeSalary DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS Books (
    BookID INT PRIMARY KEY,
    BookName VARCHAR(255),
    BookAuthor VARCHAR(255),
    BookGenre VARCHAR(255),
    ReleaseDate INT
);

CREATE TABLE IF NOT EXISTS Rentals (
    RentalID INT PRIMARY KEY,
    MemberID INT,
    EmployeeID INT,
    BookID INT,
    RentalPricePerDay DECIMAL(5, 2),
    RentalStartDate DATE,
    RentalEndDate DATE,
    TotalPrice DECIMAL(7, 2),
    FOREIGN KEY (MemberID) REFERENCES Members(MemberID),
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID),
    FOREIGN KEY (BookID) REFERENCES Books(BookID)
);
"""

# Establishing the connection to the database
try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    for statement in create_tables_sql.strip().split(';'):
        if statement:
            cursor.execute(statement + ';')
    connection.commit()
    print("Tables created successfully")
except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()

# Function to load the csv file into a dataframe
def load_csv_file(file_path):
    return pd.read_csv(file_path)

# Function to upsert data into a table
def upsert_table_data(cursor, table_name, df, key_columns):
    for index, row in df.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        columns = ', '.join(row.index)
        update_clause = ', '.join([f"{col} = VALUES({col})" for col in row.index if col not in key_columns])
        sql = (f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) "
               f"ON DUPLICATE KEY UPDATE {update_clause}")
        cursor.execute(sql, tuple(row))

# Function to synchronize the Excel data with the database
def synchronize_with_database():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Load the Excel file
        csv_file_path = './rentals.csv'
        df_excel = load_csv_file(csv_file_path)

        # Extract relevant columns from the Excel
        members_excel = df_excel[['MemberID', 'MemberName', 'MemberLastDateSubscribed']].drop_duplicates()
        employees_excel = df_excel[['EmployeeID', 'EmployeeName', 'EmployeeEmployedDate', 'EmployeeSalary']].drop_duplicates()
        books_excel = df_excel[['BookID', 'BookName', 'BookAuthor', 'BookGenre', 'ReleaseDate']].drop_duplicates()
        rentals_excel = df_excel[['RentalID', 'MemberID', 'EmployeeID', 'BookID', 'RentalPricePerDay', 'RentalStartDate', 'RentalEndDate', 'TotalPrice']]

        # Compare and update database
        upsert_table_data(cursor, 'Members', members_excel, key_columns=['MemberID'])
        upsert_table_data(cursor, 'Employees', employees_excel, key_columns=['EmployeeID'])
        upsert_table_data(cursor, 'Books', books_excel, key_columns=['BookID'])
        upsert_table_data(cursor, 'Rentals', rentals_excel, key_columns=['RentalID'])

        connection.commit()
        print("Database updated successfully")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Main loop to run the synchronization every 5 minutes
while True:
    synchronize_with_database()
    time.sleep(30)  # Sleep for 300 seconds (5 minutes)