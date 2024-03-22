import psycopg2
from contextlib import contextmanager

database='postgres-one'

@contextmanager
def create_connection(db_file):
    """ create a database connection to a PostgreSQL database """
    conn = psycopg2.connect(
        dbname='postgres-one',
        user='postgres',
        password='mysecretpassword',
        host='127.0.0.1'
        )
    yield conn
    conn.rollback()
    conn.close()