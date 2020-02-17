CREATE TABLE vedtaksperiode_tilstand
(
    vedtaksperiode_id         VARCHAR(64)                 NOT NULL,
    tilstand                  VARCHAR(64)                 NOT NULL,
    endringstidspunkt         TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (vedtaksperiode_id)
);
