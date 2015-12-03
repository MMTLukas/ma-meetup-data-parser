import json
import psycopg2
import psycopg2.extras
import sys
import os
import zipfile

import config

def setup_database(con, cursor):
    cursor.execute("DROP TABLE IF EXISTS Venues CASCADE")
    cursor.execute("DROP TABLE IF EXISTS Events CASCADE")

    cursor.execute("CREATE TABLE Venues(\
                    city VARCHAR(64),\
                    name VARCHAR(255),\
                    zip VARCHAR(32),\
                    repinned BOOLEAN,\
                    lon FLOAT,\
                    state VARCHAR(8),\
                    address_1 VARCHAR(255),\
                    country VARCHAR(8),\
                    lat FLOAT,\
                    id INT PRIMARY KEY\
                   )")

    cursor.execute("CREATE TABLE Events(\
                   status VARCHAR(16),\
                   rating_count INT,\
                   rating_average DECIMAL,\
                   utc_offset INT,\
                   event_url VARCHAR(256),\
                   description TEXT,\
                   created BIGINT,\
                   updated BIGINT,\
                   visibility VARCHAR(16),\
                   yes_rsvp_count INT,\
                   time BIGINT,\
                   waitlist_count INT,\
                   headcount INT,\
                   maybe_rsvp_count INT,\
                   id INT PRIMARY KEY,\
                   name VARCHAR(256),\
                   group_id INT,\
                   FOREIGN KEY(group_id) REFERENCES Groups(id),\
                   venue_id INT,\
                   FOREIGN KEY(venue_id) REFERENCES Venues(id) ON DELETE CASCADE\
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
                for attribute in ["status", "rating_count", "rating_average", "tc_offset", "event_url", "group_id", "description", "created",\
                               "updated", "visibility", "yes_rsvp_count", "time", "waitlist_count", "headcount", "maybe_rsvp_count", "id",\
                               "name"]:
					if not attribute in event:
						event[attribute] = ""

                if not event["id"].isdigit():
                    continue

                # Update venue
                if "venue" in event:
                    venue = event["venue"]

                    for attribute in ["zip", "state", "city", "name", "address_1", "country"]:
                        if not attribute in venue:
                            venue[attribute] = None
                    cursor.execute("INSERT INTO Venues(city, name, zip, repinned, lon, state, address_1, country, lat, id) SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s WHERE NOT EXISTS (SELECT id FROM Venues WHERE id=%s)", (venue["city"], venue["name"], venue["zip"], venue["repinned"], venue["lon"], venue["state"], venue["address_1"], venue["country"], venue["lat"], str(venue["id"]), str(venue["id"])))
                else:
                    venue = {"id": None }

                cursor.execute("INSERT INTO Events(\
                               status,\
                               rating_count,\
                               rating_average,\
                               utc_offset,\
                               event_url,\
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
                               venue_id,\
                               name) SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s WHERE NOT EXISTS\
                               (SELECT id FROM Events WHERE id=%s)", (event["status"],\
                                                                      event["rating"]["count"],\
                                                                      event["rating"]["average"],\
                                                                      event["utc_offset"],\
                                                                      event["event_url"],\
                                                                      event["description"],\
                                                                      event["created"],\
                                                                      event["updated"],\
                                                                      event["visibility"],\
                                                                      event["yes_rsvp_count"],\
                                                                      event["time"],\
                                                                      event["waitlist_count"],\
                                                                      event["headcount"],\
                                                                      event["maybe_rsvp_count"],\
                                                                      int(event["id"]),\
                                                                      venue['id'],\
                                                                      event["name"],\
                                                                      int(event["id"])))


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

