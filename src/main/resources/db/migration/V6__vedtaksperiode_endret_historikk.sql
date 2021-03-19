CREATE TABLE vedtaksperiode_endret_historikk
(
    vedtaksperiode_id       UUID        NOT NULL,
    forrige                 VARCHAR(64) NOT NULL,
    gjeldende               VARCHAR(64) NOT NULL,
    endringstidspunkt       TIMESTAMP   NOT NULL,
    endringstidspunkt_nanos BIGINT      NOT NULL
);
CREATE INDEX idx_vedtaksperiode_endret_historikk_id ON vedtaksperiode_endret_historikk (vedtaksperiode_id);
CREATE UNIQUE INDEX idx_vedtaksperiode_endret_historikk_tid ON vedtaksperiode_endret_historikk(endringstidspunkt,endringstidspunkt_nanos);