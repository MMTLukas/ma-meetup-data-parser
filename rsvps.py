import json
import psycopg2
import psycopg2.extras
import sys
import os
import zipfile

import config

def setup_database(con, cursor):

    cursor.execute("DROP TABLE IF EXISTS RSVPS")

    cursor.execute("CREATE TABLE RSVPS(\
                    event_id INT,\
                    FOREIGN KEY (event_id) REFERENCES Events(id) ON DELETE CASCADE,\
                    member_id INT,\
                    FOREIGN KEY (member_id) REFERENCES Members(id) ON DELETE CASCADE,\
                    mtime BIGINT,\
                    response VARCHAR(8),\
                    created BIGINT\
                   )")

    con.commit()


def write_rsvps(con, cursor):

    counter = 1
    file_count = len(os.listdir('./data/rsvps/'))

    for zip_files in os.listdir('./data/rsvps/'):

        print "Inserting file " + str(counter) + " of " + str(file_count) + " files..."
        counter += 1

        zip_file = zipfile.ZipFile('./data/rsvps/' + zip_files)
        for file_name in zip_file.namelist():

                data = zip_file.read(file_name)
                rsvps = json.loads(data)

                for rsvp in rsvps:

                    event = rsvp["event"]
                    if not event["id"].isdigit():
                        continue

                    # Update member
                    member = rsvp["member"]
                    cursor.execute("INSERT INTO Members(name, id) SELECT %s,%s WHERE NOT EXISTS (SELECT id FROM Members WHERE id=%s)", (member["name"], int(member["member_id"]), member["member_id"]))

                    # Update event
                    cursor.execute("INSERT INTO Events(event_url, id, name, time) SELECT %s,%s,%s,%s WHERE NOT EXISTS (SELECT id FROM Events WHERE id=%s)", (event["event_url"], event["id"], event["name"], event["time"], event["id"]))

                    # Insert Events_Members
                    if not "guest" in rsvp:
                        rsvp["guest"] = None

                    cursor.execute("INSERT INTO RSVPS(\
                        event_id,\
                        member_id,\
                        mtime,\
                        response,\
                        created\
                        ) VALUES (%s, %s, %s, %s, %s)", (
                        event["id"], 
                        member["member_id"],  
                        rsvp["mtime"], 
                        rsvp["response"], 
                        rsvp["created"]
                        ))

                con.commit()

if __name__ == "__main__":

    try:
        con = psycopg2.connect(database=config.db, user=config.user, password=config.password, host=config.host)
        cursor = con.cursor()

        setup_database(con, cursor)
        write_rsvps(con, cursor)

    except psycopg2.DatabaseError, e:

        print 'Error %s' % e
        sys.exit(1)

    finally:
        if con:
            con.close()

