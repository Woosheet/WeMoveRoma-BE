-- Storico del servizio osservato nel feed TripUpdates.
-- Cosa si misura e come e' stato verificato: PunctualityCollector e PunctualityService.

-- Un passaggio per ogni fermata servita da ogni corsa: il ritardo e' l'ultima
-- previsione del feed prima che il mezzo passasse alla fermata successiva.
-- Diviso per mese: i dati oltre i 90 giorni si tolgono staccando una partizione
-- intera, non cancellando milioni di righe. Le partizioni le crea il backend.
CREATE TABLE passaggi (
    giorno        date        NOT NULL, -- giorno di servizio: data (Roma) in cui la corsa e' comparsa nel feed
    osservato_il  timestamptz NOT NULL,
    linea         text        NOT NULL,
    route_id      text        NOT NULL,
    corsa         text        NOT NULL,
    fermata       text        NOT NULL,
    veicolo       text,
    ritardo_sec   integer     NOT NULL  -- negativo = in anticipo
) PARTITION BY RANGE (giorno);

CREATE INDEX passaggi_linea_giorno ON passaggi (linea, giorno);
CREATE INDEX passaggi_fermata_giorno ON passaggi (fermata, giorno);
-- Le righe arrivano in ordine di giorno: un BRIN costa pochi KB e basta ai filtri per data.
CREATE INDEX passaggi_giorno_brin ON passaggi USING brin (giorno);

-- Una riga per corsa vista nel feed, anche senza passaggi registrati: e' la base
-- per confrontare le corse effettuate con quelle programmate.
CREATE TABLE corse_osservate (
    giorno              date        NOT NULL,
    corsa               text        NOT NULL,
    linea               text        NOT NULL,
    route_id            text        NOT NULL,
    veicolo             text,
    prima_vista         timestamptz NOT NULL,
    ultima_vista        timestamptz NOT NULL,
    fermate_registrate  integer     NOT NULL DEFAULT 0,
    ultimo_ritardo_sec  integer,
    PRIMARY KEY (giorno, corsa)
);

CREATE INDEX corse_osservate_linea_giorno ON corse_osservate (linea, giorno);

-- Aggregato per giorno, linea e ora: si tiene per sempre, i passaggi 90 giorni.
-- Le soglie delle categorie sono quelle di PunctualityService.
CREATE TABLE puntualita_giornaliera (
    giorno         date     NOT NULL,
    linea          text     NOT NULL,
    ora            smallint NOT NULL,
    campioni       integer  NOT NULL,
    anticipo       integer  NOT NULL,
    puntuale       integer  NOT NULL,
    ritardo        integer  NOT NULL,
    forte_ritardo  integer  NOT NULL,
    PRIMARY KEY (giorno, linea, ora)
);
