import csv
import psycopg2

class DatabaseManager:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None

    def connect(self):
        # Connect to the database
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def disconnect(self):
        # Close the database connection
        if self.conn is not None:
            self.conn.close()

    def create_tables(self):
        self.connect()

        cursor = self.conn.cursor()

        # Create customers table
        create_customers_table = """
            CREATE TABLE IF NOT EXISTS customers (
                cust_id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                gender VARCHAR(10),
                phone VARCHAR,
                email VARCHAR(50),
                age INTEGER,
                state VARCHAR
            );
        """
        cursor.execute(create_customers_table)

        # Create products table
        create_products_table = """
            CREATE TABLE IF NOT EXISTS products (
                product_id SERIAL PRIMARY KEY,
                name VARCHAR,
                product_type VARCHAR,
                price DECIMAL
            );
        """
        cursor.execute(create_products_table)

        # Create sales table
        create_sales_table = """
            CREATE TABLE IF NOT EXISTS sales (
                sales_id SERIAL PRIMARY KEY,
                date TIMESTAMP,
                buyer_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                unit_price DECIMAL,
                delivery_type VARCHAR,
                delivery_cost DECIMAL,
                total DECIMAL,
                FOREIGN KEY(buyer_id) REFERENCES customers (cust_id),
                FOREIGN KEY(product_id) REFERENCES products (product_id)
            );
        """
        cursor.execute(create_sales_table)

        # Commit the changes
        self.conn.commit()

        self.disconnect()

    def load_customer_from_csv(self, table_name, csv_file):
        self.connect()

        cursor = self.conn.cursor()

        # Open the CSV file
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row

            for row in reader:
                try:
                    cust_id, name, gender, phone, email, age, state = row
                    insert_query = "INSERT INTO customers (cust_id, name, gender, phone, email, age, state) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(insert_query, (cust_id, name, gender, phone, email, age, state))
                except Exception as e:
                    pass

            # Commit the changes
            self.conn.commit()

            self.disconnect()

    def load_data_from_csv_products(self, table_name, csv_file):
        self.connect()

        cursor = self.conn.cursor()

        # Open the CSV file
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row

            # Construct the INSERT query for products table
            insert_query = "INSERT INTO products (product_id, name, product_type, price) VALUES (%s, %s, %s, %s)"

            # Load data from CSV into the products table
            for row in reader:
                try:
                    product_data = (int(row[0]), row[1], row[2], float(row[3]))
                    cursor.execute(insert_query, product_data)
                except Exception as e:
                    pass

        # Commit the changes
        self.conn.commit()

        self.disconnect()

    def load_data_from_csv_sales(self, table_name, csv_file):
        self.connect()

        cursor = self.conn.cursor()

        # Open the CSV file
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row

            # Construct the INSERT query for sales table
            insert_query = "INSERT INTO sales (sales_id, date, buyer_id, product_id, quantity, unit_price, delivery_type, delivery_cost, total) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

            # Load data from CSV into the sales table
            for row in reader:
                try:
                    sales_data = (int(row[0]), row[1], int(row[2]), int(row[3]), int(row[4]), float(row[5]), row[6], float(row[7]), float(row[8]))
                    cursor.execute(insert_query, sales_data)
                except Exception as e:
                    pass

        # Commit the changes
        self.conn.commit()

        self.disconnect()
        

if __name__ == "__main__":
    # Create an instance of DatabaseManager
    db_manager = DatabaseManager(
        host="127.0.0.1",
        port="5432",
        database="oycfw",
        user="tofunmi",
        password="toffy123"
    )

    # Create tables
    db_manager.create_tables()
    # Load data from CSV into customers table
    db_manager.load_customer_from_csv("customers", "table_data_in_csv/customers_table.csv")
    db_manager.load_data_from_csv_products("products", "table_data_in_csv/products_table.csv")
    db_manager.load_data_from_csv_sales("sales", "table_data_in_csv/sales_table.csv")

