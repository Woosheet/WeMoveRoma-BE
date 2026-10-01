-- Dove i bus perdono tempo: fra due fermate consecutive della stessa corsa, di
-- quanto e' cresciuto il ritardo. Lo calcola SlowSegmentsService dai passaggi,
-- una volta al giorno; i passaggi si cancellano dopo 90 giorni, questo resta
-- piu' a lungo (vedi punctuality.segments-retention-days).

-- Una riga per giorno di servizio, tratto e fascia oraria. Il tipo di giorno
-- (feriale, sabato, festivo) non si salva: si ricava dalla data con
-- ServiceDayType, cosi' un festivo aggiunto al calendario vale anche per il
-- passato.
CREATE TABLE tratti_giornalieri (
    giorno         date     NOT NULL,
    da             text     NOT NULL, -- fermata di partenza del tratto (stop_id)
    a              text     NOT NULL, -- fermata di arrivo
    fascia         text     NOT NULL, -- mattina (7-10), sera (16-20), resto
    corse          integer  NOT NULL,
    sec_persi      bigint   NOT NULL, -- somma, su tutte le corse, della crescita del ritardo: negativa se recuperano
    sec_persi_p90  integer  NOT NULL, -- 1 corsa su 10 perde almeno questo
    linee          text[]   NOT NULL,
    -- Basta la chiave: le letture chiedono sempre un insieme di giorni, e il
    -- giorno ne e' la prima colonna.
    PRIMARY KEY (giorno, da, a, fascia)
);
