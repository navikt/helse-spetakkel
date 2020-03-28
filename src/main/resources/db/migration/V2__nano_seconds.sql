ALTER TABLE vedtaksperiode_tilstand ADD COLUMN endringstidspunkt_nanos BIGINT NOT NULL DEFAULT 0;
ALTER TABLE vedtaksperiode_tilstand ALTER COLUMN endringstidspunkt_nanos DROP DEFAULT;
