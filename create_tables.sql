DROP TABLE IF EXISTS stops;
CREATE TABLE stops (
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

DROP TABLE IF EXISTS buses;
CREATE TABLE buses (
    Recorded_At timestamp with time zone,
    Line varchar,
    Direction int,
    Date date,
    Lon numeric,
    Lat numeric,
    Delay int,
    Departure_Time varchar(4),
    PRIMARY KEY(Recorded_At, Line, Direction, Date, Departure_Time)
);

DROP TABLE IF EXISTS testing_table;
CREATE TABLE testing_table (
    col1 varchar
);
