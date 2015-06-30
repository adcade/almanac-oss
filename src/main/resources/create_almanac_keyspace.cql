CREATE KEYSPACE IF NOT EXISTS almanac WITH REPLICATION = {
    'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1
};

USE almanac;

DROP TABLE IF EXISTS metrics;

CREATE TABLE metrics (
    bucket text,
    geohash varchar,
    factkey varchar,
    span int,
    timestamp timestamp,
    total counter,
    count counter,
    PRIMARY KEY ((bucket, geohash), span, timestamp, factkey)
) WITH CLUSTERING ORDER BY (span DESC, timestamp DESC);

DROP TABLE IF EXISTS facts;

CREATE TABLE facts (
    bucket text,
    geohash varchar,
    factkey varchar,
    facts map<text, text>,
    PRIMARY KEY ((bucket, geohash), factkey)
);