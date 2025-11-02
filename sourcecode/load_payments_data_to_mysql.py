import mysql.connector
import pandas as pd

hostname="xjz4pu.h.filess.io"
username="olister_victorydog"
password="1bb4881add44b4fe62002ee01c909632e050c5f5"
database_name="olister_victorydog"
portname="61002"

try:
    # Establish the connection

    mydb = mysql.connector.connect(
        host=hostname,
        user=username,
        password=password,
        database=database_name,
        port=portname
    )

    if mydb.is_connected():
        cursor = mydb.cursor()
        print("Connection successful!")

        # You can now execute SQL queries

        cursor.execute("DROP TABLE IF EXISTS olist_order_payments;")
        print("Table olist_order_payments has been dropped!")
        cursor.execute("""
            CREATE TABLE olist_order_payments(
                order_id VARCHAR(255) NOT NULL,
                payment_sequential INT,
                payment_type VARCHAR(50),
                payment_installments INT,
                payment_value DECIMAL(8,2)
            );
        """)

        mydb.commit()
        print("table olist_order_payments created")
        cursor.execute("SHOW TABLES;")
        print(cursor.fetchall())

        # loading data into pandas dataframe
        data = pd.read_csv(r'C:\Users\pedan\OneDrive\Desktop\Data Engineering\Pythonproject\otherImpStuff\olist_order_payments_dataset.csv')

        # need to write a code to load data in batches to avoid data loading failure
        # batch_size = 1000
        # total_records = len(data)

        values = [
            tuple(row) for row in data.itertuples(name=None, index=False)
        ]
        print(len(values))

        cursor.executemany("INSERT INTO olist_order_payments VALUES (%s, %s, %s, %s, %s)", values)
        mydb.commit()


except mysql.connector.Error as ex:
    sqlstate = ex.args[0]
    print(f"Connection failed: {sqlstate}")

except Exception as e:
    print(f"Connection failed: {e}")

finally:
    # Close the connection
    if 'mydb' in locals() and mydb is not None and mydb.is_connected():
        cursor.close()
        mydb.close()
        print("Connection closed.")
