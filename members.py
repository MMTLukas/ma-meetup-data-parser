import json
import psycopg2
import psycopg2.extras
import sys
import os
import config

def setup_database(con, cursor):
    cursor.execute("DROP TABLE IF EXISTS Members CASCADE")
    cursor.execute("DROP TABLE IF EXISTS Members_Topics")
    cursor.execute("DROP TABLE IF EXISTS Member_Photos")
    cursor.execute("DROP TABLE IF EXISTS Topics CASCADE")

    cursor.execute("CREATE TABLE Members(\
				status VARCHAR(32),\
				city VARCHAR(127),\
				name VARCHAR(255),\
				tumblr TEXT,\
				twitter TEXT,\
				flickr TEXT,\
				facebook TEXT,\
				bio TEXT,\
				hometown VARCHAR(127),\
				lon NUMERIC,\
				joined BIGINT,\
				state VARCHAR(8),\
				link TEXT,\
				lat NUMERIC,\
				visited BIGINT,\
				country VARCHAR(8),\
				id INT PRIMARY KEY,\
				photo_id INT,\
	            thumb_link VARCHAR(127),\
	            photo_link VARCHAR(127),\
	            highres_link VARCHAR(127)\
	           )")
    cursor.execute("CREATE TABLE Topics(\
	            id INT PRIMARY KEY,\
	            urlkey VARCHAR(64),\
	            name VARCHAR(127)\
	           )")
    cursor.execute("CREATE TABLE Members_Topics(\
	            member_id INT,\
	            FOREIGN KEY (member_id) REFERENCES Members(id) ON DELETE CASCADE,\
	            topic_id INT,\
	            FOREIGN KEY (topic_id) REFERENCES Topics(id) ON DELETE CASCADE\
	           )")
    cursor.execute("CREATE TABLE Member_Photos(\
	            photo_id INT PRIMARY KEY,\
	            member_id INT,\
	            FOREIGN KEY (member_id) REFERENCES Members(id) ON DELETE CASCADE,\
	            thumb_link VARCHAR(127),\
	            photo_link VARCHAR(127),\
	            highres_link VARCHAR(127)\
	           )")

    con.commit()

def write_members(con, cursor):
    counter = 1
    file_count = len(os.listdir('./data/members_updated/'))
    member_services = {}

    for file_name in os.listdir('./data/members_updated/'):

        print "Inserting file " + str(counter) + " of " + str(file_count) + " files..."
        counter += 1

        with open('./data/members_updated/' + str(file_name)) as data_file:
            data = json.load(data_file)

            for member in data:
                # check missing values
                for attrubute in ["state", "bio", "country", "hometown", "city"]:
                    if not attrubute in member:
                        member[attrubute] = ""

                # check missing social services
                for service in ["tumblr", "twitter", "flickr", "facebook"]:
                    if service in member["other_services"]:
                        member_services[service] = member["other_services"][service]["identifier"]
                    else:
                        member_services[service] = ""

                        # Update members
                cursor.execute("INSERT INTO Members(\
					status,\
					city,\
					name,\
					tumblr,\
					twitter,\
					flickr,\
					facebook,\
					bio,\
					hometown,\
					lon,\
					joined,\
					state,\
					link,\
					lat,\
					visited,\
					country,\
					id\
					) SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s \
					WHERE NOT EXISTS(SELECT id FROM members WHERE id = %s)", (
                    member["status"],
                    member["city"],
                    member["name"],
                    member_services["tumblr"],
                    member_services["twitter"],
                    member_services["flickr"],
                    member_services["facebook"],
                    member["bio"],
                    member["hometown"],
                    member["lon"],
                    member["joined"],
                    member["state"],
                    member["link"],
                    member["lat"],
                    member["visited"],
                    member["country"],
                    member["id"],
                    member["id"]
                ))

                # Update topics
                for topic in member["topics"]:
                    cursor.execute(
                        "INSERT INTO Topics(id, urlkey, name) SELECT %s,%s,%s WHERE NOT EXISTS (SELECT id FROM Topics WHERE id=%s)",
                        (topic["id"], topic["urlkey"], topic["name"], topic["id"]))
                    cursor.execute("INSERT INTO Members_Topics(member_id, topic_id) VALUES(%s, %s)",
                                   (member["id"], topic["id"]))


                    # Update photos
                if "photo" in member:
                    if not "highres_link" in member["photo"]:
                        member["photo"]["highres_link"] = ""
                    cursor.execute("INSERT INTO Member_Photos(\
					        	photo_id,\
					            member_id,\
					            thumb_link,\
					            photo_link,\
					            highres_link\
					            ) SELECT %s, %s, %s, %s, %s \
								WHERE NOT EXISTS(SELECT photo_id FROM Member_Photos WHERE photo_id = %s)", ( \
                        member["photo"]["photo_id"], \
                        member["id"], \
                        member["photo"]["thumb_link"], \
                        member["photo"]["photo_link"], \
                        member["photo"]["highres_link"], \
                        member["photo"]["photo_id"] \
                        ))

                con.commit()


if __name__ == "__main__":

    try:
        con = psycopg2.connect(database=config.db, user=config.user, password=config.password, host=config.host)
        cursor = con.cursor()

        setup_database(con, cursor)
        write_members(con, cursor)

    except psycopg2.DatabaseError, e:

        print 'Error %s' % e
        sys.exit(1)

    finally:
        if con:
            con.close()
