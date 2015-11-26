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

neo4j-import --into meetup.db --multiline-fields true --id-type Integer \
    --nodes:TOPICS data/export/topics.csv \
    --nodes:GROUPS data/export/groups.csv \
    --nodes:CATEGORIES data/export/categories.csv \
    --nodes:MEMBERS data/export/members.csv \
    --nodes:EVENTS data/export/events.csv \
    --nodes:VENUES data/export/venues.csv \
    --relationships:IS_ABOUT data/export/groups_topics.csv \
    --relationships:IN_CATEGORY data/export/groups_categories.csv \
    --relationships:INTERESTED_IN data/export/members_topics.csv \
    --relationships:LOCATED_AT data/export/events_venues.csv \
    --relationships:RSVP data/export/events_members.csv \
    --relationships:HOSTED_BY data/export/events_groups.csv

neo4j restart