CREATE KEYSPACE IF NOT EXISTS almanac WITH REPLICATION = {
    'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1
};

USE almanac;

CREATE TABLE IF NOT EXISTS metrics (
    factkey varchar,
    bucket text,
    geohash varchar,
    span int,
    timestamp timestamp,
    total counter,
    count counter,
    PRIMARY KEY ((factkey, bucket, geohash), span, timestamp)
) WITH CLUSTERING ORDER BY (span DESC, timestamp DESC);

CREATE TABLE IF NOT EXISTS facts (
    bucket text,
    geohash varchar,
    factkey varchar,
    facts map<text, text>,
    PRIMARY KEY ((bucket, geohash), factkey)
);

CREATE INDEX IF NOT EXISTS ON facts (keys(facts));