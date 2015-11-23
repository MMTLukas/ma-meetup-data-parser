#! /bin/bash
rm -rf meetup.db

## tables
# Members
# Topics
# Groups
# Events
# Categories
# Venues

## relationships
# groups_topics
# groups_categories
# members_topics
# events_venues
# rsvps
# events_groups

/opt/neo4j/bin/neo4j-import --into meetup.db --multiline-fields true --id-type Integer \
    --nodes:Topics data/export/topics.csv \
    --nodes:Groups data/export/groups.csv \
    --nodes:Categories data/export/categories.csv \
    --nodes:Members data/export/members.csv \
    --nodes:Events data/export/events.csv \
    --nodes:Venues data/export/venues.csv \
    --relationships:HAVE data/export/groups_topics.csv \
    --relationships:HAS data/export/groups_categories.csv \
    --relationships:HAVE data/export/members_topics.csv \
    --relationships:HAVE data/export/events_venues.csv \
    --relationships:VISIT data/export/rsvps.csv \
    --relationships:BELONGS_TO data/export/events_groups.csv \

/opt/neo4j/bin/neo4j restart