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

def randomize_suppliers():
    name = [fake.name() for _ in range(100)]
    contact_info = [fake.phone_number() for _ in range(100)]
    country = [fake.country() for _ in range(100)]
    return name, contact_info, country

def randomize_products():
    name = [fake.name() for _ in range(total_users)]
    category = [fake.email() for _ in range(total_users)]
    price = [fake.date_between(start_date='-25y', end_date='-1y') for _ in range(total_users)]
    supplier_id = [fake.date_between(start_date='-25y', end_date='-1y') for _ in range(total_users)]
    stock_quantity = [fake.date_between(start_date='-25y', end_date='-1y') for _ in range(total_users)]
    return name, category, price, supplier_id, stock_quantity

def randomize_customers():
    name = [fake.name() for _ in range(total_users)]
    location = [fake.email() for _ in range(total_users)]
    email = [fake.date_between(start_date='-25y', end_date='-1y') for _ in range(total_users)]
    return name, location, email

def randomize_orders():
    customer_id = [fake.name() for _ in range(total_users)]
    order_date = [fake.email() for _ in range(total_users)]
    order_status = [fake.date_between(start_date='-25y', end_date='-1y') for _ in range(total_users)]
    return customer_id, order_date, order_status

def randomize_order_items():
    order_id = [fake.name() for _ in range(total_users)]
    product_id = [fake.email() for _ in range(total_users)]
    quantity = [fake.date_between(start_date='-25y', end_date='-1y') for _ in range(total_users)]
    price_at_purchase = [fake.date_between(start_date='-25y', end_date='-1y') for _ in range(total_users)]
    return order_id, product_id, quantity, price_at_purchase

def randomize_shipment():
    order_id = [fake.name() for _ in range(total_users)]
    shipped_date = [fake.email() for _ in range(total_users)]
    delivery_date = [fake.date_between(start_date='-25y', end_date='-1y') for _ in range(total_users)]
    shipping_cost = [fake.date_between(start_date='-25y', end_date='-1y') for _ in range(total_users)]
    return order_id, shipped_date, delivery_date, shipping_cost

def randomize():
    randomize_suppliers()
    randomize_products()
    randomize_customers()
    randomize_orders()
    randomize_order_items()
    randomize_shipment()

def main():
    randomize()


if __name__ == "__main__":
    main()