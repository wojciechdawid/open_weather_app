CREATE TABLE IF NOT EXISTS weather_10km_current(
    id bigint GENERATED ALWAYS AS IDENTITY,
    id_geom bigint,
    temperature real,
    temperature_max real,
    temperature_min real,
    humidity real,
    pressure real,
    wind_speed real,
    wind_direction real,
    clouds real,
    visibility real,
    sunrise timestamp without time zone,
    sunset timestamp without time zone,
    last_update timestamp without time zone,
    name character varying(40),
    country character varying(40),
    weather_type character varying(40),
    weather_code integer,
    weather_desc character varying(40),
    precip_type character varying(40),
    precip_value real,
    CONSTRAINT weather_10km_current_pkey PRIMARY KEY (id),
    CONSTRAINT weather_10km_current_fkey
        FOREIGN KEY(id_geom)
            REFERENCES poland_10km(id)
);
COMMIT;