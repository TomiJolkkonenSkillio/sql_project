import psycopg2
from config import config
from database_population import randomize_data
from analytical_queries import *
from datetime import datetime


def database_design():
    con = None
    try:
        con = psycopg2.connect(**config())  
        cursor = con.cursor()

        # the database query
        SQL = '''

        -- Create table Suppliers
        CREATE TABLE IF NOT EXISTS Suppliers (
            id SERIAL PRIMARY KEY,
            name varchar(255) NOT NULL,
            contact_info varchar(255), -- ?
            country varchar(255)
        );

        -- Create table Products
        CREATE TABLE IF NOT EXISTS Products (
            id SERIAL PRIMARY KEY,
            name varchar(255) NOT NULL,
            category varchar(255),
            price NUMERIC(10, 2),
            supplier_id int NOT NULL,
            stock_quantity int,
            FOREIGN KEY (supplier_id) REFERENCES Suppliers(id)
        );

        -- Create table Customers
        CREATE TABLE IF NOT EXISTS Customers (
            id SERIAL PRIMARY KEY,
            name varchar(255) NOT NULL,
            location varchar(255), --?
            email varchar(255) NOT NULL
        );

        -- Create table Orders
        CREATE TABLE IF NOT EXISTS Orders (
            id SERIAL PRIMARY KEY,
            customer_id int NOT NULL,
            order_date DATE NOT NULL,
            order_status varchar(255) NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES Customers(id) -- order_status; ordered, fulfilled, delivered? or what?
        );

        -- Create table Order_items
        CREATE TABLE IF NOT EXISTS Order_items (
            id SERIAL PRIMARY KEY,
            order_id int NOT NULL,
            product_id int NOT NULL,
            quantity int NOT NULL,
            price_at_purchase NUMERIC(10, 2),
            FOREIGN KEY (order_id) REFERENCES Orders(id),
            FOREIGN KEY (product_id) REFERENCES Products(id)
        );

        -- Create table Shipments
        CREATE TABLE IF NOT EXISTS Shipments (
            id SERIAL PRIMARY KEY,
            order_id int NOT NULL,
            shipped_date DATE NOT NULL,
            delivery_date DATE NOT NULL,
            shipping_cost NUMERIC(10, 2),
            FOREIGN KEY (order_id) REFERENCES Orders(id)
        );
        '''
        cursor.execute(SQL)
        con.commit()

        # Close cursor
        cursor.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def main():
    #database_design()
    randomize_data()
    analyze()

if __name__ == "__main__":
    main()