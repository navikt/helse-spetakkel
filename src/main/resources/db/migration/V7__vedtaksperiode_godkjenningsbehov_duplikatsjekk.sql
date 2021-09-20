CREATE TABLE vedtaksperiode_godkjenningsbehov_duplikatsjekk
(
    vedtaksperiode_id UUID PRIMARY KEY,
    innsatt TIMESTAMP DEFAULT now()
);