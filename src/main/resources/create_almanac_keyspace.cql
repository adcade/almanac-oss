CREATE KEYSPACE IF NOT EXISTS almanac WITH REPLICATION = {
    'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1
};

USE almanac;

DROP TABLE IF EXISTS metrics;

CREATE TABLE metrics (
    bucket text,
    factkey varchar,
    geohash varchar,
    span int,
    timestamp timestamp,
    total counter,
    count counter,
    PRIMARY KEY ((bucket, geohash), span, timestamp, factkey)
);

DROP TABLE IF EXISTS facts;

CREATE TABLE facts (
    bucket text,
    factkey varchar,
    facts map<text, text> static,
    PRIMARY KEY (bucket, factkey)
);