import json
import psycopg2
import psycopg2.extras
import sys
import os

import config

def setup_database(con, cursor):

    cursor.execute("DROP TABLE IF EXISTS Group_Photos CASCADE")
    cursor.execute("DROP TABLE IF EXISTS Groups_Topics")
    cursor.execute("DROP TABLE IF EXISTS Groups CASCADE")
    cursor.execute("DROP TABLE IF EXISTS Categories")

    cursor.execute("CREATE TABLE Categories(\
                    id INT PRIMARY KEY,\
                    shortname VARCHAR(32),\
                    name VARCHAR(127)\
                   )")
    cursor.execute("CREATE TABLE Groups( \
                    category_id INT,\
                    FOREIGN KEY (category_id) REFERENCES Categories(id),\
                    city VARCHAR(127),\
                    rating FLOAT(2),\
                    description TEXT,\
                    join_mode VARCHAR(32),\
                    country VARCHAR(8),\
                    who VARCHAR(127),\
                    lon NUMERIC,\
                    visibility VARCHAR(32),\
                    created BIGINT,\
                    state VARCHAR(8),\
                    link TEXT,\
                    members INT,\
                    urlname VARCHAR(64),\
                    lat NUMERIC,\
                    timezone VARCHAR(32),\
                    organizer_id INT,\
                    FOREIGN KEY (organizer_id) REFERENCES Members(id),\
                    id INT PRIMARY KEY,\
                    name VARCHAR(255)\
                   )")
    cursor.execute("CREATE TABLE Groups_Topics(\
                    group_id INT,\
                    FOREIGN KEY (group_id) REFERENCES Groups(id) ON DELETE CASCADE,\
                    topic_id INT,\
                    FOREIGN KEY (topic_id) REFERENCES Topics(id) ON DELETE CASCADE\
                   )")
    cursor.execute("CREATE TABLE Group_Photos(\
                    photo_id INT PRIMARY KEY,\
                    group_id INT,\
                    FOREIGN KEY (group_id) REFERENCES Groups(id) ON DELETE CASCADE,\
                    thumb_link VARCHAR(127),\
                    photo_link VARCHAR(127),\
                    highres_link VARCHAR(127)\
                   )")

    con.commit()

def write_groups(con, cursor):

    counter = 1;
    file_count = len(os.listdir('./data/categories_and_groups/'))

    for file_name in os.listdir('./data/categories_and_groups/'):

        print "Inserting file " + str(counter) + " of " + str(file_count) + " files..."
        counter += 1

        with open('./data/categories_and_groups/' + str(file_name)) as data_file:
            data = json.load(data_file)

            for group in data:

                # Update categories
                category_id = group["category"]["id"]
                cursor.execute("INSERT INTO Categories(shortname, id, name) SELECT %s,%s,%s WHERE NOT EXISTS (SELECT id FROM Categories WHERE id=%s)", (group["category"]["shortname"], category_id, group["category"]["name"], category_id))

                # Update members
                organizer_id = group["organizer"]["member_id"]
                cursor.execute("INSERT INTO Members(name, id) SELECT %s,%s WHERE NOT EXISTS (SELECT id FROM Members WHERE id=%s)", (group["organizer"]["name"], organizer_id, organizer_id))

                if not "description" in group:
                    group["description"] = ""

                # Update groups
                cursor.execute("INSERT INTO Groups(category_id, city, rating, description, join_mode, country, who, lon, visibility, created, state, link, members, urlname, lat, timezone, organizer_id, id, name) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (category_id, group["city"], group["rating"], group["description"], group["join_mode"], group["country"], group["who"], group["lon"], group["visibility"], group["created"], group["state"], group["link"], group["members"], group["urlname"], group["lat"], group["timezone"], organizer_id, group["id"], group["name"]))

                # Update topics
                for topic in group["topics"]:
                    cursor.execute("INSERT INTO Topics(id, urlkey, name) SELECT %s,%s,%s WHERE NOT EXISTS (SELECT id FROM Topics WHERE id=%s)", (topic["id"], topic["urlkey"], topic["name"], topic["id"]))
                    cursor.execute("INSERT INTO Groups_Topics(group_id, topic_id) VALUES(%s, %s)", (group["id"], topic["id"]))

                # Update photos
                if "group_photo" in group:
                    group_photo_id = group["group_photo"]["photo_id"]
                    cursor.execute("INSERT INTO Group_Photos(thumb_link, photo_id, photo_link, highres_link, group_id) SELECT %s,%s,%s,%s,%s WHERE NOT EXISTS (SELECT id FROM Group_Photos WHERE id=%s)", (group["group_photo"]["thumb_link"], group_photo_id, group["group_photo"]["photo_link"], group["group_photo"]["highres_link"], group["id"], group_photo_id))

            con.commit()


if __name__ == "__main__":

    try:
        con = psycopg2.connect(database=config.db, user=config.user, password=config.password, host=config.host)
        cursor = con.cursor()

        setup_database(con, cursor)
        write_groups(con, cursor)

    except psycopg2.DatabaseError, e:

        print 'Error %s' % e
        sys.exit(1)

    finally:
        if con:
            con.close()

