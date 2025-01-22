import psycopg2
from config import config
from analytical_queries import *
from database_population import *

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
    database_design() # database degisn queries
    #randomize_data() # randomized data for realistic distribution
    #data_consistency() # data consistency (e.g. order_date should be earlier than delivery_date)
    #populate_100() # populate with at least 100 entries in the Orders, Order-Items, Customers tables
    #basic_counts_sums() # tot no. of orders, tot sales, products w. low stock
    #grouping_aggregations() # tot. sales per product category, avr. order value, monthly breakdown of no. of orders and tot. sales
    #joins_multitablequeries() # list of orders, each order shows cust. name and tot. order value, top 5 customers tot. spending, supplier list w. details
    #nextedqueries_subqueries() # non-ordered products, customer over x€ spent per order, orders w. biggest quantity
    #advanced_analyticalqueries() # daily orders trend, peak order days, avr. delivery time per month, % of tot. sales filtered by top 10% of product sales.

if __name__ == "__main__":
    main()