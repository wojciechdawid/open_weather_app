CREATE INDEX IF NOT EXISTS weather_10km_current_id_idx ON weather_10km_current USING btree(id);
CREATE INDEX IF NOT EXISTS weather_10km_current_id_geom_idx ON weather_10km_current USING btree(id_geom);

CREATE INDEX IF NOT EXISTS weather_maz_10km_current_id_idx ON weather_maz_10km_current USING btree(id);
CREATE INDEX IF NOT EXISTS weather_maz_10km_current_id_geom_idx ON weather_maz_10km_current USING btree(id_geom);

CREATE INDEX IF NOT EXISTS mazowieckie_10km_id_idx ON mazowieckie_10km USING btree(id);