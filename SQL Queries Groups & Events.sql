# GROUPS

SELECT * FROM groups LIMIT 100 OFFSET 0;
SELECT * FROM groups WHERE id = 23713;

### Gruppe mit eigener URL abfragen
SELECT g.*, (c.name) as category, (p.id) as photo_id, p.thumb_link, p.photo_link, p.highres_link
FROM groups g, photos p, categories c
WHERE p.group_id = g.id AND c.id = g.category_id AND g.id = 23713;

### groups + topics
SELECT t.name
FROM groups g, groups_topics gt, topics t
WHERE g.id = 23713 AND g.id = gt.group_id AND t.id = gt.topic_id;

SELECT t.name
FROM groups g INNER JOIN groups_topics gt ON g.id = gt.group_id INNER JOIN topics t ON gt.topic_id = t.id
WHERE g.id = 23713;

### EVENTS

SELECT * FROM events LIMIT 100 OFFSET 0;
SELECT * FROM events WHERE id = '195572882';
SELECT * FROM events e JOIN groups g ON e.group_id = g.id WHERE e.id = '80260272';
SELECT * FROM events e JOIN groups g ON e.group_id = g.id LEFT OUTER JOIN venues v ON e.venue_id = v.id WHERE e.id = '80260272';

### RSVPS per year/month

SELECT count(*) FROM RSVPS
WHERE DATE_PART('year', TO_TIMESTAMP(mtime / 1000)) = 2012
AND DATE_PART('month', TO_TIMESTAMP(mtime / 1000)) = 10
LIMIT 10;

### RSVPs per group / category

SELECT * FROM RSVPS r JOIN groups g ON r.group_id = g.id WHERE g.id = 15792002 LIMIT 10;
SELECT * FROM RSVPS r JOIN groups g ON r.group_id = g.id JOIN categories c ON g.category_id = c.id WHERE c.id = 31 LIMIT 10;


### Visualisierung
SELECT count(*), DATE_PART('year', TO_TIMESTAMP(r.mtime / 1000)) FROM RSVPS r 
	JOIN groups g ON r.group_id = g.id 
	JOIN categories c ON g.category_id = c.id 
	WHERE c.id = 31 group by DATE_PART('year', TO_TIMESTAMP(r.mtime / 1000)) order by DATE_PART DESC;

SELECT count(*), DATE_PART('month', TO_TIMESTAMP(r.mtime / 1000)) as month, DATE_PART('year', TO_TIMESTAMP(r.mtime / 1000)) as year
 FROM RSVPS r 
	JOIN groups g ON r.group_id = g.id 
	JOIN categories c ON g.category_id = c.id 
	WHERE c.id = 31 group by DATE_PART('month', TO_TIMESTAMP(r.mtime / 1000)), DATE_PART('year', TO_TIMESTAMP(r.mtime / 1000)) order by year DESC, month DESC;


###
SELECT count(*), DATE_PART('year', TO_TIMESTAMP(r.mtime / 1000)) FROM RSVPS r 
	JOIN groups g ON r.group_id = g.id 
	JOIN categories c ON g.category_id = c.id 
	WHERE g.id = 23713 group by DATE_PART('year', TO_TIMESTAMP(r.mtime / 1000)) order by DATE_PART DESC;

SELECT count(*), DATE_PART('month', TO_TIMESTAMP(r.mtime / 1000)) as month, DATE_PART('year', TO_TIMESTAMP(r.mtime / 1000)) as year
 FROM RSVPS r 
	JOIN groups g ON r.group_id = g.id 
	JOIN categories c ON g.category_id = c.id 
	WHERE c.id = 31 group by DATE_PART('month', TO_TIMESTAMP(r.mtime / 1000)), DATE_PART('year', TO_TIMESTAMP(r.mtime / 1000)) order by year DESC, month DESC;

