import json
import psycopg2
import psycopg2.extras
import sys
import os
import zipfile

import config

def setup_database(con, cursor):
    cursor.execute("DROP TABLE IF EXISTS Events")

    cursor.execute("CREATE TABLE Events(\
                   status VARCHAR(16),\
                   rating_count INT,\
                   rating_average DECIMAL,\
                   utc_offset INT,\
                   event_url VARCHAR(256),\
                   group_id INT,\
                   FOREIGN KEY(group_id) REFERENCES Groups(id),\
                   description TEXT,\
                   created BIGINT,\
                   updated BIGINT,\
                   visibility VARCHAR(16),\
                   yes_rsvp_count INT,\
                   time BIGINT,\
                   waitlist_count INT,\
                   headcount INT,\
                   maybe_rsvp_count INT,\
                   id VARCHAR(32) PRIMARY KEY,\
                   name VARCHAR(256)\
                   )")

    con.commit()

def write_events(con, cursor):

    file_count = len(os.listdir('./data/events/'))
    counter = 1

    for file_name in os.listdir("./data/events/"):

        print "Inserting file " + str(counter) + " of " + str(file_count) + " files..."
        counter += 1

        with open('./data/events/' + str(file_name)) as data_file:
            data = json.load(data_file)

            for event in data:
                for attrubute in ["status", "rating_count", "rating_average", "tc_offset", "event_url", "group_id", "description", "created",\
                               "updated", "visibility", "yes_rsvp_count", "time", "waitlist_count", "headcount", "maybe_rsvp_count", "id",\
                               "name"]:
					if not attrubute in event:
						event[attrubute] = ""


                cursor.execute("INSERT INTO Events(\
                               status,\
                               rating_count,\
                               rating_average,\
                               utc_offset,\
                               event_url,\
                               group_id,\
                               description,\
                               created,\
                               updated,\
                               visibility,\
                               yes_rsvp_count,\
                               time,\
                               waitlist_count,\
                               headcount,\
                               maybe_rsvp_count,\
                               id,\
                               name) SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s WHERE NOT EXISTS\
                               (SELECT id FROM Events WHERE id=%s)", (event["status"],\
                                                                      event["rating"]["count"],\
                                                                      event["rating"]["average"],\
                                                                      event["utc_offset"],\
                                                                      event["event_url"],\
                                                                      event["group"]["id"],\
                                                                      event["description"],\
                                                                      event["created"],\
                                                                      event["updated"],\
                                                                      event["visibility"],\
                                                                      event["yes_rsvp_count"],\
                                                                      event["time"],\
                                                                      event["waitlist_count"],\
                                                                      event["headcount"],\
                                                                      event["maybe_rsvp_count"],\
                                                                      str(event["id"]),\
                                                                      event["name"],\
                                                                      str(event["id"])))


            con.commit()

if __name__ == "__main__":

    try:
        con = psycopg2.connect(database=config.db, user=config.user, password=config.password, host=config.host)
        cursor = con.cursor()

        setup_database(con, cursor)
        write_events(con, cursor)

    except psycopg2.DatabaseError, e:

        print 'Error %s' % e
        sys.exit(1)

    finally:
        if con:
            con.close()

