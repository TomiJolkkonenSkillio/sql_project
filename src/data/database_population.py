import psycopg2
import numpy as np
import pandas as pd
from config import config
import random
from faker import Faker

'''
    randomize_data() # randomized data for realistic distribution
    data_consistency() # data consistency (e.g. order_date should be earlier than delivery_date)
    populate_100() # populate with at least 100 entries in the Orders, Order-Items, Customers tables

'''

fake = Faker()

# Suppliers
def randomize_suppliers(cursor, num_suppliers):
    for i in range(num_suppliers):
        name = fake.company()
        contact_info = fake.phone_number()
        country = fake.country()
    
        sql = 'INSERT INTO Suppliers (name, contact_info, country) VALUES (%s, %s, %s);'
        cursor.execute(sql, (name, contact_info, country))

# Customers
def randomize_customers(cursor, num_customers):
    for i in range(num_customers):
        name = fake.name()
        location = fake.address().replace("\n", ", ")
        email = fake.email()

        sql = 'INSERT INTO Customers (name, location, email) VALUES (%s, %s, %s);'
        cursor.execute(sql, (name, location, email))

# Products
def randomize_products(cursor, num_products, num_suppliers):
    for i in range(num_products):
        name = fake.word()
        category = random.choice(['Clothes', 'Hardware', 'Cleaning supplies', 'Bags', 'Hats', 'Shoes'])
        price = round(random.uniform(10.0, 500.0), 2)
        supplier_id = random.randint(1, num_suppliers)
        stock_quantity = random.randint(1, 1000)

        sql = 'INSERT INTO Products (name, category, price, supplier_id, stock_quantity) VALUES (%s, %s, %s, %s, %s);'
        cursor.execute(sql, (name, category, price, supplier_id, stock_quantity))

# Orders
def randomize_orders(cursor, num_orders, num_customers):
    order_dates = []
    for i in range(num_orders):
        customer_id = random.randint(1, num_customers)
        order_date = fake.date_between(start_date='-5y', end_date='today')
        order_status = random.choice(['ordered', 'fulfilled', 'delivered'])

        orders_sql = 'INSERT INTO Orders (customer_id, order_date, order_status) VALUES (%s, %s, %s);'
        cursor.execute(orders_sql, (customer_id, order_date, order_status))

        order_dates.append(order_date)
    return order_dates

# Order_items
def randomize_order_items(cursor, num_order_items, num_orders, num_products):
    for i in range(num_order_items):
        order_id = random.randint(1, num_orders)
        product_id = random.randint(1, num_products)
        quantity = random.randint(1, 10)
        price_at_purchase = round(random.uniform(10.0, 500.0), 2)

        sql = 'INSERT INTO Order_items (order_id, product_id, quantity, price_at_purchase) VALUES (%s, %s, %s, %s);'
        cursor.execute(sql, (order_id, product_id, quantity, price_at_purchase))

# Shipments
def randomize_shipments(cursor, num_shipments, num_orders, order_dates):
    for i in range(num_shipments):
        order_id = random.randint(1, num_orders)
        order_date = order_dates[order_id - 1] # Get the order_date for this order from the list
        shipped_date = fake.date_between(start_date=order_date)
        delivery_date = fake.date_between(start_date=shipped_date)
        shipping_cost = round(random.uniform(5.0, 50.0), 2)

        sql = 'INSERT INTO Shipments (order_id, shipped_date, delivery_date, shipping_cost) VALUES (%s, %s, %s, %s);'
        cursor.execute(sql, (order_id, shipped_date, delivery_date, shipping_cost))

def randomize_data():
    con = None
    cursor = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()

        num_suppliers = 150
        num_customers = 170
        num_products = 350
        num_orders = 400
        num_order_items = 300
        num_shipments = 250

        randomize_suppliers(cursor, num_suppliers)
        randomize_customers(cursor, num_customers)
        randomize_products(cursor, num_products, num_suppliers)
        order_dates = randomize_orders(cursor, num_orders, num_customers)
        randomize_order_items(cursor, num_order_items, num_orders, num_products)
        randomize_shipments(cursor, num_shipments, num_orders, order_dates)

        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cursor is not None:
            cursor.close()
        if con is not None:
            con.close()