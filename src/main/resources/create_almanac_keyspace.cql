CREATE KEYSPACE IF NOT EXISTS almanac WITH REPLICATION = {
	'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1
};

USE almanac;

DROP TABLE IF EXISTS metrics;

CREATE TABLE metrics (
	factkey varchar,
	facts map<text, text> static,
	span int,
	geohash varchar,
	ts timestamp,
	bucket text static,
	total int,
	count int,
	max int,
	min int,
	PRIMARY KEY ((bucket, geohash), span, ts, factkey)
);