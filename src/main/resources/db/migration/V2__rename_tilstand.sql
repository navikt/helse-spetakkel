UPDATE vedtaksperiode_tilstand SET tilstand='MOTTATT_SYKMELDING' WHERE tilstand='MOTTATT_NY_SØKNAD';
UPDATE vedtaksperiode_tilstand SET tilstand='AVVENTER_SØKNAD' WHERE tilstand='AVVENTER_SENDT_SØKNAD';
