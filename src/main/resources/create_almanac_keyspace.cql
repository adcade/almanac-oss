CREATE KEYSPACE IF NOT EXISTS almanac WITH REPLICATION = {
	'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1
};

DROP TABLE IF EXISTS almanac.metrics;

CREATE TABLE almanac.metrics (
	factkey varchar,
	date int,
	facts map<text, text> static,
	span int,
	timestamp timestamp,
	key text,
	total int,
	count int,
	max int,
	min int,
	PRIMARY KEY ((factkey, date), span, timestamp, key)
) WITH CLUSTERING ORDER BY (span DESC, timestamp DESC, key ASC);