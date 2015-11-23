import json
import psycopg2
import psycopg2.extras
import sys
import os
import re
import config

def export_tables(con, cursor):
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

    tables = cursor.fetchall()

    for table in tables:
        table = re.sub(r'[^\w]', '', str(table))
        print "Exporting " + table + " to ./data/export/" + table + ".csv"

        if table != "events":
            continue

        with open("./data/export/" + table + ".csv", "w") as data_file:
            cursor.copy_expert("COPY " + table + " TO STDOUT WITH CSV HEADER", data_file)

    con.commit()


if __name__ == "__main__":

    try:
        con = psycopg2.connect(database=config.db, user=config.user, password=config.password, host=config.host)
        cursor = con.cursor()

        export_tables(con, cursor)

    except psycopg2.DatabaseError, e:

        print 'Error %s' % e
        sys.exit(1)

    finally:
        if con:
            con.close()
