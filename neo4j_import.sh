#! /bin/bash
rm -rf meetup.db

neo4j-import --into meetup.db --multiline-fields true --id-type Integer \
    --nodes:Members data/export/members.csv \
    --nodes:Topics data/export/topics.csv \
    --relationships:CONTAINS data/export/members_topics.csv

neo4j restart