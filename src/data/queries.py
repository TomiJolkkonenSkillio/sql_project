import psycopg2
from config import config

def query_person():
    con = None
    try:
        # connect to the database
        con = psycopg2.connect(**config())
        cursor = con.cursor()

        # the actual query, selecting all from table 'person'
        SQL = 'SELECT * FROM person;'
        cursor.execute(SQL)

        # fetch all rows from the result
        row = cursor.fetchall()
        for i in row:
            print(i)

        # close cursor
        cursor.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def main():
    query_person() 

if __name__ == "__main__":
    main()