import pandas as pd
import mysql.connector

# Define MySQL connection parameters
mysql_config = {
    'host': 'localhost',
    'database': 'test',
    'user': 'root',
    'password': 'password'
}

# Read Excel file into a Pandas DataFrame
df = pd.read_excel('data.xlsx')

# Create a MySQL connection
cnx = mysql.connector.connect(**mysql_config)

# Iterate over the DataFrame rows
for i, row in df.iterrows():
    # Check if the row already exists in the MySQL table
    cur = cnx.cursor()
    cur.execute("SELECT * FROM my_table WHERE col1 = %s AND col2 = %s AND col3 = %s", (row['col1'], row['col2'], row['col3']))
    result = cur.fetchone()
    if result:
        # Row already exists, skip it
        print(f"Row {i+1} already exists in the MySQL table, skipping...")
    else:
        # Row does not exist, insert it to the MySQL table
        cur.execute("INSERT INTO my_table (col1, col2, col3) VALUES (%s, %s, %s)", (row['col1'], row['col2'], row['col3']))
        cnx.commit()
        print(f"Row {i+1} inserted to the MySQL table")

# Close the MySQL connection
cnx.close()


'''
If you have millions of rows in the Excel file, reading the entire file into memory can be inefficient. In that case, you can try to use batch processing and multiprocessing to improve performance. Here's an example code to insert the data from the Excel file to MySQL using batch processing:
'''

import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from multiprocessing import Pool, cpu_count

# Define database connection details
config = {
  'user': 'your_username',
  'password': 'your_password',
  'host': 'your_host',
  'database': 'your_database',
  'raise_on_warnings': True
}

# Define function to insert data to MySQL
def insert_to_mysql(batch):
    try:
        # Connect to MySQL
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()

        # Prepare SQL statement for inserting data
        sql = "INSERT INTO your_table (column1, column2, column3) VALUES (%s, %s, %s)"

        # Execute SQL statement for each row in batch
        for row in batch.itertuples(index=False):
            cursor.execute(sql, tuple(row))

        # Commit the changes
        cnx.commit()

        # Close the connection
        cursor.close()
        cnx.close()
        return True

    except mysql.connector.Error as err:
        # Handle the error
        print(err)
        return False

# Read the Excel file in chunks
chunksize = 100000
for chunk in pd.read_excel('your_excel_file.xlsx', chunksize=chunksize):
    # Check if the rows already exist in MySQL
    existing_rows = pd.read_sql_query('SELECT * FROM your_table WHERE column1 IN %s', con=mysql.connector.connect(**config), params=[tuple(chunk['column1'].tolist())])

    # Filter out the rows that already exist
    new_rows = chunk[~chunk['column1'].isin(existing_rows['column1'])]

    # Insert the new rows using multiprocessing
    pool = Pool(processes=cpu_count())
    results = pool.map(insert_to_mysql, [new_rows[i:i+100] for i in range(0, len(new_rows), 100)])
    pool.close()
    pool.join()

    # Check if all the new rows have been inserted successfully
    if False in results:
        print('Error: some rows failed to insert')


'''
In this example code, we are reading the Excel file in chunks of 100000 rows and checking if the rows already exist in MySQL using a SELECT query with IN clause. We then filter out the rows that already exist and insert the new rows using multiprocessing with a batch size of 100. We also check if all the new rows have been inserted successfully before moving to the next chunk. This can help to avoid duplicate entries and improve the performance of the insert process.
'''
