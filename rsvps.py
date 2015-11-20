import json
import psycopg2
import psycopg2.extras
import sys
import os
import zipfile

import config

def setup_databases(con, cursor):

    cursor.execute("DROP TABLE IF EXISTS RSVPS")
    cursor.execute("DROP TABLE IF EXISTS Events")
    cursor.execute("DROP TABLE IF EXISTS Venues")

    cursor.execute("CREATE TABLE Events(\
                    id VARCHAR(32) PRIMARY KEY,\
                    event_url VARCHAR(255),\
                    name VARCHAR(255),\
                    time BIGINT\
                   )")

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
                    id VARCHAR(32) PRIMARY KEY\
                   )")

    cursor.execute("CREATE TABLE RSVPS(\
                    group_id INT,\
                    FOREIGN KEY (group_id) REFERENCES Groups(id) ON DELETE CASCADE,\
                    venue_id VARCHAR(32),\
                    FOREIGN KEY (venue_id) REFERENCES Venues(id) ON DELETE CASCADE,\
                    created BIGINT,\
                    id INT PRIMARY KEY,\
                    mtime BIGINT,\
                    response VARCHAR(8),\
                    member_id INT,\
                    FOREIGN KEY (member_id) REFERENCES Members(id) ON DELETE CASCADE,\
                    guest INT,\
                    photo_id INT,\
                    FOREIGN KEY (photo_id) REFERENCES Photos(id) ON DELETE SET NULL,\
                    event_id VARCHAR(32),\
                    FOREIGN KEY (event_id) REFERENCES Events(id) ON DELETE CASCADE\
                   )")

    con.commit()

def write_rsvps(con, cursor):

    for zip_files in os.listdir('./data/rsvps/'):
        zip_file = zipfile.ZipFile('./data/rsvps/' + zip_files)
        for file_name in zip_file.namelist():

                data = zip_file.read(file_name)
                data = json.loads(data)

                for rsvps in data:

                    # Todo Tallies?!

                    # Update member
                    member = rsvps["member"]
                    cursor.execute("INSERT INTO Members(name, id) SELECT %s,%s WHERE NOT EXISTS (SELECT id FROM Members WHERE id=%s)", (member["name"], int(member["member_id"]), member["member_id"]))

                    # Update group
                    group = rsvps["group"]
                    cursor.execute("INSERT INTO Groups(urlname, lon, id, lat, join_mode) SELECT %s,%s,%s,%s,%s WHERE NOT EXISTS (SELECT id FROM Groups WHERE id=%s)", (group["urlname"], group["group_lon"], group["id"], group["group_lat"], group["join_mode"], group["id"]))

                    # Update venue
                    if "venue" in rsvps:
                        venue = rsvps["venue"]
                        if not "zip" in venue:
                            venue["zip"] = None
                        if not "state" in venue:
                            venue["state"] = None
                        cursor.execute("INSERT INTO Venues(city, name, zip, repinned, lon, state, address_1, country, lat, id) SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s WHERE NOT EXISTS (SELECT id FROM Venues WHERE id=%s)", (venue["city"], venue["name"], venue["zip"], venue["repinned"], venue["lon"], venue["state"], venue["address_1"], venue["country"], venue["lat"], str(venue["id"]), str(venue["id"])))

                    # Update photo
                    if "member_photo" in rsvps:
                        photo = rsvps["member_photo"]
                        if not "highres_link" in photo:
                            photo["highres_link"] = ""
                        cursor.execute("INSERT INTO Photos(thumb_link, id, photo_link, highres_link) SELECT %s,%s,%s,%s WHERE NOT EXISTS (SELECT id FROM Photos WHERE id=%s)", (photo["thumb_link"], photo["photo_id"], photo["photo_link"], photo["highres_link"], photo["photo_id"]))

                    # Update event
                    event = rsvps["event"]
                    cursor.execute("INSERT INTO Events(event_url, id, name, time) SELECT %s,%s,%s,%s WHERE NOT EXISTS (SELECT id FROM Events WHERE id=%s)", (event["event_url"], event["id"], event["name"], event["time"], event["id"]))

                con.commit()

if __name__ == "__main__":

    try:
        con = psycopg2.connect(database=config.db, user=config.user, password=config.password, host=config.host)
        cursor = con.cursor()

        setup_databases(con, cursor)
        write_rsvps(con, cursor)

    except psycopg2.DatabaseError, e:

        print 'Error %s' % e
        sys.exit(1)

    finally:
        if con:
            con.close()

