CREATE TABLE vedtaksperiode_tilstand
(
    vedtaksperiode_id VARCHAR(64)                 NOT NULL,
    tilstand          VARCHAR(64)                 NOT NULL,
    timeout           BIGINT                      NOT NULL DEFAULT 0,
    endringstidspunkt TIMESTAMP WITHOUT TIME ZONE NOT NULL,

    PRIMARY KEY (vedtaksperiode_id)
);

CREATE TABLE slack_thread
(
    vedtaksperiode_id VARCHAR(64)                 NOT NULL,
    thread_ts         VARCHAR(64)                 NOT NULL,
    opprettet         TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (vedtaksperiode_id)
);
