CREATE INDEX IF NOT EXISTS weather_5km_current_id_idx ON weather_5km_current USING btree(id);
CREATE INDEX IF NOT EXISTS weather_5km_current_id_geom_idx ON weather_5km_current USING btree(id_geom);