import psycopg2
from config import config
from analytical_queries import *
from database_population import *

def database_design():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()

        # database design queries
        SQL = 'SELECT * FROM person;'
        cursor.execute(SQL)

        # close cursor
        cursor.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def main():
    database_design() # database degisn queries
    randomize_data() # randomized data for realistic distribution
    data_consistency() # data consistency (e.g. order_date should be earlier than delivery_date)
    populate_100() # populate with at least 1000 entries in the Orders, Order-Items, Customers tables
    basic_counts_sums() # tot no. of ordewrs, tot sales, products w. low stock
    grouping_aggregations() # tot. sales per product category, avr. order value, monthly breakdown of no. of orders and tot. sales
    joins_multitablequeries() # list of otders, each order shows cust. name and tot. order value, top 5 customers tot. spending, supplier list w. details
    nextedqueries_subqueries() # non-ordered products, customer over x€ spent per order, orders w. biggest quantity
    advanced_analyticalqueries() # daily orders trend, peak order days, avr. delivery time per month, % of tot. sales filtered by top 10% of product sales.

if __name__ == "__main__":
    main()