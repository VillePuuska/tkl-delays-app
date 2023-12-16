DROP TABLE IF EXISTS records;
CREATE TABLE records (
    Recorded_At timestamp with time zone,
    Line varchar,
    Direction int,
    Date date,
    Lon numeric,
    Lat numeric,
    Delay int,
    Departure_Time varchar(4),
    Stop varchar(4),
    Stop_Order int,
    PRIMARY KEY(Line, Direction, Date, Departure_Time, Stop)
);

DROP TABLE IF EXISTS latest;
CREATE TABLE latest (
    Recorded_At timestamp with time zone,
    Line varchar,
    Direction int,
    Date date,
    Lon numeric,
    Lat numeric,
    Delay int,
    Departure_Time varchar(4),
    PRIMARY KEY(Line, Direction, Date, Departure_Time)
);