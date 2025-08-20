import psycopg2


def connector():
    conn = psycopg2.connect(
        dbname="NewDB",
        user="postgres",
        password="Geirby12",
        host="localhost",
        port="5432"
    )
    return conn
