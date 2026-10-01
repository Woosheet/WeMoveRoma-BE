# gtfs-monitor — Backend Spring Boot

Backend REST + SSE della piattaforma **WeMoveRoma**. Si occupa di ingerire i feed GTFS (statico e realtime) di ATAC / Roma Mobilità, esporli via API versionata `/api/v1`, fare da proxy verso OpenTripPlanner per il planner multimodale, gestire integrazioni accessorie (Trenitalia, geocoding) e spedire le push (avvisi di servizio, scioperi).

> Vedi anche: [workspace overview](../ARCHITECTURE.md) — [deploy](../deploy/ARCHITECTURE.md) — [OTP](../otp/ARCHITECTURE.md)
>
> Approfondimenti con le misure che motivano gli interventi descritti qui:
> [`ANALISI_API.md`](../ANALISI_API.md) (revisione contratto/robustezza, 19 agosto 2026) e
> [`ANALISI_PERFORMANCE.md`](../ANALISI_PERFORMANCE.md) (latenza e consumi, 19 luglio 2026).

> **Ultimo allineamento del documento:** 20 agosto 2026, sul codice fino al commit `4664416`
> *(feat(api): linee multiple negli avvisi, severita' derivata, validazione, cache)*.

---

## Stack

- **Spring Boot 3.5.7** su **Java 21**
- Build: **Maven** (`./mvnw package` → `target/*.jar` → `app.jar`)
- Starter: `web` (MVC), `webflux` (WebClient reattivo), `actuator`, `cache`, **`validation`** (Bean Validation sui parametri dei controller)
- **gtfs-realtime-bindings 0.0.8** + `protobuf-java 3.25.5` per il parsing dei feed PB
- **univocity-parsers 2.9.1** per il GTFS statico (CSV)
- **firebase-admin 9.4.3** per le push su topic FCM (no-op senza credenziali)
- Lombok, Jackson JSR310

---

## Configurazione

File principali in [`src/main/resources/`](src/main/resources):

- `application.properties` — base
- `application-local.properties` — dev locale (CORS `localhost:4200/4203/5173`, GTFS in `data/gtfs_static`, OTP `http://localhost:8081`)
- `application-prod.properties` — produzione (CORS `wemoveroma.com`, OTP `http://otp:8081` via rete Docker)

> `gtfs.static-props.data-dir` esiste **solo** nei due profili, non nel base: senza profilo attivo
> il bean `GtfsIndexService` non parte (vedi [Test](#test)).

### Chiavi rilevanti

| Gruppo | Chiave | Note |
|--------|--------|------|
| GTFS statico | `gtfs.static-props.url`, `.data-dir` | feed Roma Mobilità, refresh 1h; `data-dir` solo nei profili local/prod |
| GTFS realtime | `gtfs.realtime.{trip-updates,vehicle-positions,service-alerts}-url` | polling 5s |
| Scheduler | `spring.task.scheduling.pool.size=6` | senza pool esplicito Spring userebbe **un solo thread** per i 3 poll RT, i cron e gli scioperi |
| Warm-up indici | `gtfs.index.warmup.enabled` (true), `gtfs.index.warmup.days` (2) | scalda gli indici per-data a fine rebuild (vedi [Prestazioni](#prestazioni-e-caching)) |
| OTP | `journey.otp.enabled`, `journey.otp.base-url`, `journey.otp.search-window(-fallback)`, `journey.otp.max-itineraries` | search 90–180 min; il tuning a piedi/salita è in `otp/data/router-config.json`, non più nella query |
| Planner treno | `journey.otp.rail-visibility-slot` (2), `rail-visibility-max-delay-minutes` (45), `rail-injection-enabled` (true), `rail-injection-max-station-meters` (1200) | visibilità/iniezione opzione treno (vedi sezione Journey planner) |
| Rail | `rail.viaggiatreno.*` | integrazione Trenitalia (ViaggiaTreno, dati live) |
| Push FCM | `fcm.enabled`, `fcm.credentials-path`, `fcm.topic-alerts`, `fcm.topic-strikes` | senza credenziali il dispatcher è no-op: il backend gira lo stesso |
| Push avvisi | `notifications.alerts.severities` (SEVERE,WARNING), `.max-per-refresh` (5), `notifications.state-file` | filtro e backstop anti-flood (vedi [Note di design](#note-di-design)) |
| Scioperi | `strikes.mit.rss-url`, `strikes.sectors`, `strikes.regions`, `strikes.dispatch-on-startup` | i default nel codice sono più larghi di quelli in `application.properties`: vince il file |
| Storico | `spring.datasource.*` (env `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`), `punctuality.enabled` | Postgres; il backend parte e funziona anche senza (vedi [Storico su Postgres](#storico-su-postgres--punctualitycollector-punctualityservice)) |
| Web | `spring.codec.max-in-memory-size=104MB` | richiesto per protobuf grandi (ma i WebClient hanno limiti propri: vedi B12 in `ANALISI_PERFORMANCE.md`) |
| CORS | `app.cors.allowed-origins` | per profilo |

---

## Struttura dei package

```
src/main/java/.../gtfsmonitor/
├── controller/   ← 16 REST controller @RestController (13 su /api/v1 + 3 legacy grezzi)
├── service/      ← logica: indici GTFS, polling RT, SSE, planner, geocoding, alert, rail,
│                   scioperi, dispatch push
├── model/dto/    ← ~27 DTO per le risposte API
├── config/       ← WebClient, binding properties GTFS, CORS, Firebase,
│                   cache HTTP statici, exception handler
└── utils/        ← util varie (es. DelayFmt)
```

---

## REST API (`/api/v1`)

### Vehicles — `ApiVehiclesController`
- `GET /vehicles` — lista mezzi (filtri: `linea`, `destination`, `limit`)
- `GET /vehicles/stream` — **SSE** stream posizioni in tempo reale
- `GET /vehicles/{vehicleId}/next-stops` — prossime fermate (scheduled + actual). Con id `sim-<tripId>` risponde con le fermate programmate della corsa simulata. *Nessun 404: un id sconosciuto dà `200` con lista vuota.*

### Dashboard — `ApiDashboardController`
- `GET /dashboard/summary` — conteggi runtime (mezzi visibili, linee attive, ritardi, avvisi) più **`feedLastModified`**: il `Last-Modified` dichiarato da Roma Mobilità per il GTFS statico da cui vengono gli indici, in formato RFC 1123. Null finché non se ne conosce uno.

  Serve a chi rigenera gli snapshot delle pagine pubbliche del frontend: confronta questo valore con quello registrato nell'ultima rigenerazione e rifà il lavoro — 434 chiamate a `/catalog/lines/{line}/pattern` — solo se il feed è stato davvero ripubblicato. È il verso giusto della dipendenza: il backend espone uno stato che già possiede, non chiama nessuno e non custodisce credenziali di deploy.

### Stops — `ApiStopsController`
- `GET /stops` — lista (bbox o totale)
- `GET /stops/search` — ricerca per nome
- `GET /stops/{stopId}/arrivals` — arrivi previsti — **404** se la fermata non esiste, **503** se gli indici non sono ancora pronti

`/stops` e `/stops/search` usano `GtfsIndexService.dataVersion()` come `generatedAt` (non `Instant.now()`): è ciò che rende utilizzabile l'ETag, vedi [Prestazioni](#prestazioni-e-caching).

**`NearbyStopDTO`** (usato sia da `/stops/{id}/arrivals` sia da `/nearby`):

| Campo | Note |
|---|---|
| `stopId`, `stopName`, `lat`, `lon` | anagrafica fermata |
| `distanceMeters`, `walkMinutes` | valorizzati solo da `/nearby` (su `/stops/{id}/arrivals` sono `null`) |
| **`servedLines`** | linee servite dalla fermata **nella giornata di servizio**, indipendenti dagli arrivi del momento |
| `arrivals` | arrivi live + programmati fusi, entro l'orizzonte |

> `servedLines` non è un riassunto di `arrivals`: di notte `arrivals` è vuoto ma la fermata resta
> servita dalle sue linee, e gli avvisi di servizio relativi vanno mostrati comunque. È alimentato
> da `GtfsIndexService.linesServingStop(stopId, data)`, che riusa l'indice per-data già in memoria —
> nessuna scansione aggiuntiva di `stop_times.txt`.

### Journey planner — `ApiJourneyController`
- `GET /journey/plan` — pianificazione multimodale via OTP (`fromLat`, `fromLon`, `toLat`, `toLon`, `numItineraries`, `timeMode`, `when`, `modes`)

Il `JourneyPlannerService` interroga OTP (GraphQL `planConnection`) e poi **post-processa** i risultati:

- **Dedup + ranking** (`dedupeAndEnrich`): raggruppa itinerari con lo stesso pattern (linee/fermate), ordina con camminata-pura in fondo e poi per orario di arrivo; le corse equivalenti diventano `alternativeBoardingTimes`.
- **Visibilità treno** (`promoteBestRailOption`): garantisce che la migliore opzione con leg `RAIL` compaia entro `journey.otp.rail-visibility-slot` (senza scalzare la #0), se competitiva entro `rail-visibility-max-delay-minutes`.
- **Iniezione treno** (`tryBuildInjectedRailOption`): workaround per un caso limite di routing access/egress di OTP sui salti brevi (1 fermata). Quando il piano **non** contiene treni ma partenza e arrivo sono entrambi entro `rail-injection-max-station-meters` da una stazione, costruisce un'opzione sintetica *cammino → treno (stazione→stazione) → cammino*: trova le stazioni con `stopsByRadius`, pianifica il treno fra i due centri stazione, sceglie la prima corsa **realmente prendibile** (`when + cammino d'accesso`) e la inietta. Disattivabile con `journey.otp.rail-injection-enabled=false`.

> **Tuning di routing OTP:** velocità/riluttanza a piedi, costo di salita, slack, ecc. **non** viaggiano più nel blocco `preferences` per-richiesta (OTP 2.8.1 lo rifiuta) — la query del backend è senza `preferences`. Il tuning vive in [`otp/data/router-config.json`](../otp/ARCHITECTURE.md) (`routingDefaults`). Restano per-richiesta solo `searchWindow`, `dateTime`, `modes`, `first`.

### Trips — `ApiTripsController`
- `GET /trips/{tripId}/shape` — geometria polyline (*nessun 404: corsa sconosciuta → `points` vuoto*)
- `GET /trips/{tripId}/stops` — fermate schedulate (`when` opzionale, ISO instant) — **404** se la corsa non esiste, **503** se gli indici non sono pronti, **`200 []`** se la corsa esiste ma non è programmata in quella data

### Live focus su fermata — `ApiPlannerController`
- `GET /planner/live-stop-focus` — ETA real-time + stato servizio

### Nearby — `ApiNearbyController`
- `GET /nearby?lat=&lon=&radiusMeters=&limitStops=&limitArrivalsPerStop=` — fermate vicine + arrivi

Unico controller con `@Validated`: `lat` in `[-90,90]`, `lon` in `[-180,180]`, i tre limiti `@Positive`.
Vincoli violati → **400** con corpo `ProblemDetail` (vedi `ApiExceptionHandler`).

### Search / Catalog
- `GET /search/suggestions` — autocomplete linee/fermate/indirizzi
- `GET /catalog/lines` — linee disponibili
- `GET /catalog/destinations?linea=` — destinazioni di una linea
- `GET /catalog/lines/{line}/punctuality?from=&to=` — **puntualità** fra due date comprese (predefinito: ultimi 28 giorni; un solo giorno con `from = to`, oggi compreso), per ora: passaggi in anticipo (< −1 min), puntuali, in ritardo (3–10 min), in forte ritardo (≥ 10 min). `hours` somma tutti i giorni, `dayTypes` divide gli stessi passaggi in feriale/sabato/festivo. Conteggi, non percentuali. 400 fuori dai 90 giorni conservati, 404 per linea inesistente, 503 se lo storico non è raggiungibile. Vedi [Storico su Postgres](#storico-su-postgres--punctualitycollector-punctualityservice)
- `GET /catalog/lines/{line}/pattern?date=` — **percorso canonico** della linea: una voce per direzione/capolinea con le fermate in ordine, il modo (`bus`/`tram`/`metro`/`treno`), il **tracciato reale** che segue le strade e, se si passa una data, gli **orari** di quella giornata (prima corsa, ultima, numero di corse, intervallo tipico)
- `GET /catalog/stop-schedules?date=` — **orari di tutta la rete, fermata per fermata**: una riga per palina + linea + direzione con prima corsa, ultima, numero di corse, intervallo tipico. Secondi dalla mezzanotte del giorno di servizio, che possono superare le 24 ore (una corsa delle 2 di notte è scritta 26:00). Data obbligatoria, ammessa da ieri a due settimane. ~22.600 righe, 3,4 MB, ~4 s: lo chiama `fetch-lines-snapshot.mjs` tre volte — feriale, sabato, festivo — non un utente

Le `stop-schedules` esistono perché le pagine delle fermate mostravano gli orari
della **direzione**, cioè quelli del capolinea: su `/fermata/nazionale-torino` la
linea 40 risultava con prima corsa alle 06:06 mentre alla palina 70084 la prima è
alle 06:47. Tre cose non ovvie:

- **Non passa dalla cache degli indici per-data.** Quella cache tiene solo tre
  giorni: chiederle una data lontana buttava fuori l'indice di oggi, e arrivi e
  fermate vicine lo ricostruivano a ogni richiesta (misurato il 25/09/2026: da
  0,015 s a 3–6 s dopo una sola chiamata, su un endpoint pubblico). Ora l'indice
  si legge dalla cache se c'è, altrimenti si costruisce a parte e si butta via.
  Un calcolo alla volta, e il risultato resta in memoria per le ultime 4 date
  finché il feed non cambia: la seconda chiamata per la stessa data costa 40 ms.
- **Una corsa che tocca due volte la stessa palina conta una volta sola.** Il 40
  parte e torna a Termini: le 171 corse diventavano 342 e la pagina prometteva un
  bus ogni 4 minuti invece che ogni 6. Vale la prima volta, che è la partenza.
- **Gli orari possono superare le 24 ore** e restano così: una corsa delle 2 di
  notte appartiene al giorno di servizio precedente ed è scritta 26:00. Riportarli
  nelle 24 ore spetta a chi presenta il dato, che è l'unico a sapere se sta
  scrivendo un orario singolo o un intervallo che passa la mezzanotte.

Il `pattern` alimenta le [pagine pubbliche per linea](../GTFS-Monitor-FE/GTFS-MONITOR-FE/ARCHITECTURE.md#pagine-per-linea--linea64-lineamea)
del frontend, che di una linea devono poter mostrare **testo**: un elenco di
fermate, non una mappa. Tre scelte non ovvie:

- **Indipendente dalla data, di proposito.** Costruirlo con `scheduledStopsForTrip`
  sarebbe stato naturale ma sbagliato: quel metodo filtra per validita' del
  servizio e restituisce vuoto per ogni linea le cui corse campionate non girano
  oggi — verificato sulla 160, che risultava senza fermate. Il percorso di una
  linea non e' l'orario di oggi, e infatti le fermate non portano orari: un
  orario senza data di servizio non significa niente.
- **Campionamento distribuito, non le prime N.** Le corse sono ordinate per
  `service_id`, quindi leggere la testa della lista significa guardare sempre lo
  stesso giorno-tipo. Su ogni gruppo direzione+capolinea si campiona a passo
  costante e si tiene la corsa **piu' lunga**: la 160 ha rami da 22 e da 37
  fermate, e senza spaziare il campionamento uscirebbe il ramo corto.
- **404 su linea inesistente, 503 a indici vuoti.** Un elenco vuoto sarebbe
  indistinguibile da una linea reale senza corse; e finche' gli indici non sono
  carichi nessun id esiste, quindi rispondere 404 sarebbe una bugia (stessa
  convenzione di `ApiStopsController`).

Il modo arriva da `route_type` di `routes.txt`, caricato negli indici insieme al
resto. Serve a non intitolare "Linea MEA" una pagina che ogni romano cerca come
"metro A".

Sul resto della risposta, due note:

- **Il tracciato** e' quello della corsa campione, gia' in memoria ed esposto da
  `shapeByTripId`: unire le fermate con segmenti dritti darebbe un percorso che
  taglia per i campi. Le coordinate sono arrotondate a cinque decimali (~1 m),
  perche' il feed le serializza con quattordici e le linee lunghe superano i
  cinquecento punti.
- **Gli orari** si calcolano solo se arriva una data, e **una sola per
  richiesta**: la cache degli indici per-data tiene il giorno richiesto e i due
  adiacenti, quindi chiederne tre lontani fra loro la farebbe ricostruire ogni
  volta, un secondo e mezzo e decine di MB per volta. L'intervallo e' la
  **mediana** degli scarti fra partenze, non la media: su una linea che di notte
  si dirada, un solo buco di tre ore sposterebbe la media a un valore che non
  descrive nessun momento reale della giornata.

Alimenta anche le [pagine per fermata](../GTFS-Monitor-FE/GTFS-MONITOR-FE/SEO.md)
del frontend, che ricavano da qui — invertendo i percorsi — quali linee servono
ogni fermata, senza bisogno di un endpoint dedicato.

### Alerts — `ApiAlertsController`
- `GET /alerts` — avvisi di servizio (filtri: `linea`, `active` (default `true`), `limit`)

**`ApiAlertDTO`** — campi rilevanti:

| Campo | Note |
|---|---|
| `alertId` | id entity del feed |
| `line` | **prima** linea di `lines`. Mantenuto per compatibilità, di fatto deprecato |
| **`lines`** | *tutte* le linee coinvolte (`ServiceAlertDTO.routeIds` per intero) |
| **`severity`** | `INFO` / `WARNING` / `SEVERE`, **mai null**. Dichiarata dal feed se c'è, altrimenti derivata |
| **`severitySource`** | `FEED` o `DERIVED` — dice sempre quale dei due casi |
| **`declaredSeverity`** | ciò che dichiara il feed, verbatim (`null` se non lo dichiara: a Roma è sempre) |
| **`cause`** / **`effect`** | etichette italiane (es. "Lavori", "Deviazione") |
| **`causeCode`** / **`effectCode`** | codici GTFS-RT non tradotti (`CONSTRUCTION`, `DETOUR`) — **da preferire per qualunque logica** |
| `title`, `description`, `startsAt`, `endsAt`, `updatedAt` | invariati |

Perché queste tre severità e non una vedi [Note di design](#note-di-design).

### Scioperi — `ApiStrikesController`
- `GET /strikes` — scioperi rilevanti, già filtrati lato server per settore + regione
- `POST /strikes/refresh` — force-refresh manuale (debug). **Non protetto**: se l'endpoint diventa raggiungibile dall'esterno va messo dietro auth

### Geocode — `ApiGeocodeController`
- `GET /geocode/search` — forward (indirizzo → coord)
- `GET /geocode/reverse` — reverse (coord → indirizzo)

### Rail — `ApiRailController`
- `GET /rail/train-info` — stato treno Trenitalia (`trainNumber`, `stationName`, `referenceTime`)

### Dashboard — `ApiDashboardController`
- `GET /dashboard/summary` — metriche aggregate. **Nessun client lo chiama** (§7 di [`ANALISI_API.md`](../ANALISI_API.md)): o gli si dà uno scopo o si rimuove

### Endpoint legacy (fuori da `/api/v1`)
`GET /vehicle-positions`, `GET /trip-updates`, `GET /service-alerts` — proiezioni grezze degli snapshot GTFS-RT, precedenti all'API versionata. Restano utili per il debug; non fanno parte del contratto verso i client.

---

## Background jobs, push e SSE

### Polling GTFS Realtime
`VehiclePositionsService`, `TripUpdatesService`, `ServiceAlertsService` usano `@Scheduled` con `fixedDelay=5s` e mantengono lo stato in `AtomicReference` con lock di refresh. Il pool dello scheduler è a **6 thread** (`spring.task.scheduling.pool.size`): prima i tre poll, i cron e la pubblicazione SSE si serializzavano su un thread solo.

### Static GTFS — `StaticGtfsUpdater`
- `@EventListener(ApplicationReadyEvent)` — fetch all'avvio, su thread dedicato
- `@Scheduled(cron="0 40 6,20 * * *", zone="Europe/Rome")` — refresh giornaliero 06:40 e 20:00
- `@Scheduled(fixedDelay=5min)` — retry su errore
- Cron e retry **non girano sul pool dello scheduler**: sottomettono il lavoro pesante (download zip + parsing CSV + rebuild indici) all'executor dedicato `static-gtfs-refresh` e ritornano subito

### Scioperi MIT — `StrikeService`
- `@Scheduled(cron="0 0 6,14 * * *", zone="Europe/Rome")` — parsing del feed RSS di `scioperi.mit.gov.it`
- Warm-up a 60 s dal boot che popola il tracker **senza dispatchare** (`strikes.dispatch-on-startup=false`): al primo avvio tutti gli scioperi sono "nuovi", notificarli sarebbe spam
- Filtro settore/regione lato server. In `application.properties` i settori sono ristretti a *Trasporto Pubblico Locale* e *Trasporto ferroviario*: "Ferroviario" generico e "Appalti ferroviari" (default più larghi nel codice) coprono anche scioperi di ristorazione e pulizie, che **non fermano le corse**

### Storico su Postgres — `PunctualityCollector`, `PunctualityService`

Registra il servizio **osservato**, che il feed in tempo reale non conserva:

| Tabella | Contenuto | Conservazione |
|---|---|---|
| `passaggi` | una riga per fermata servita da ogni corsa: linea, corsa, fermata, mezzo, ritardo in secondi. Partizionata per mese | 90 giorni (si stacca la partizione) |
| `corse_osservate` | una riga per corsa vista nel feed, con prima/ultima vista e passaggi registrati: la base per confrontare corse effettuate e programmate | 90 giorni |
| `puntualita_giornaliera` | aggregato per giorno, linea e ora | per sempre |

- **Cosa si misura.** Per ogni corsa, il ritardo previsto alla prossima fermata; si registra l'ultimo valore visto quando quella fermata **sparisce dall'elenco delle rimanenti**, cioè quando il mezzo ci è passato, una volta sola per corsa. Non basta che la prossima fermata cambi: il feed a volte torna indietro (38, 39, 38, 39), e contare ogni cambio produceva il 4,3% di doppioni. Si scartano la prima fermata vista (capolinea o riavvio) e i ritardi oltre ±60 min (mezzo abbinato alla corsa sbagliata).
- **Verificato sui dati** (settembre 2026, 5.629 passaggi reali in 12 minuti, confrontati col GTFS statico): 1 doppione su 5.629; fermate trovate il 91% di quelle programmate, perché il feed si aggiorna ogni 30 s e le fermate superate in meno tempo non compaiono mai; ritardo registrato entro 2 min da "ora di passaggio − orario programmato" nel 96,8% dei casi; query dell'API, ricalcolo indipendente in Python e aggregato giornaliero identici cella per cella. Residuo noto (~0,3%): una corsa che sparisce dal feed per più di un minuto e ricompare più indietro viene trattata come nuova.
- **Verificato sul campo** (settembre 2026): su 45 mezzi entro 60 m da una fermata, ritardo del feed e ritardo osservato (ora GPS − orario programmato) differivano in mediana di 0,3–0,5 min, 41 su 45 entro 2 min. Nello stesso campione **la maggioranza dei bus era in anticipo** sull'orario pubblicato (mediana −4,4 min): non è un errore del feed.
- **Due thread.** Il campionamento (ogni 15 s, solo memoria) legge l'istantanea già scaricata da `TripUpdatesService.snapshot()`, mai un download nuovo. Le scritture girano su un thread proprio (`puntualita-db`), per non occupare il pool dello scheduler.
- **Il backend non dipende dal database.** `initialization-fail-timeout=-1`, connessione in 2 s al massimo, migrazione Flyway lanciata dal servizio quando Postgres risponde (non da Spring all'avvio), coda di 60.000 passaggi (~90 min). Provato spegnendo Postgres col backend acceso: mezzi e arrivi 200, puntualità 503 in 2 s, e alla riaccensione i passaggi in coda scritti senza buchi.
- **Tipo di giorno a calendario** (`ServiceDayType`): domeniche, festività nazionali e Pasquetta sono festivi. Provato e scartato il calendario del GTFS (settembre 2026): identificativi dei servizi con convenzioni diverse per operatore e copertura parziale classificavano un giovedì come sabato. Il 29 giugno non è incluso perché non verificato; a settembre 2026 i servizi ATAC portano il tipo nel prefisso (`10#` feriale, `20#` sabato, `30#` festivo), utile per controllarlo quando il feed coprirà fine giugno.
- **Manutenzione oraria** sul thread di scrittura: partizioni del mese corrente e del successivo, aggregato dei due giorni precedenti (idempotente), eliminazione oltre i 90 giorni.
- In locale: `DB_URL=jdbc:postgresql://127.0.0.1:5432/wemoveroma` e le altre variabili; in produzione il servizio `postgres` di [deploy/docker-compose.prod.yml](../deploy/docker-compose.prod.yml).

### Push FCM — `FcmDispatcherService`, `AlertsDispatchTracker`, `StrikesDispatchTracker`
- Invio a topic (`wemoveroma-alerts`, `wemoveroma-strikes`). Senza credenziali o con `fcm.enabled=false` è **no-op**: il backend resta pienamente funzionante
- I tracker persistono su file JSON gli id già notificati, così un restart non ri-notifica tutto
- Chiave di dispatch avvisi: `id@inizio_epoch` — stesso id con nuovo periodo di attività è un avviso nuovo
- Backstop `notifications.alerts.max-per-refresh=5` per ciclo (5 s)
- **Filtro avvisi (settembre 2026).** Prima leggeva la severità *dichiarata* dal feed, che a Roma non arriva mai: nessuna push partiva. Ora legge quella **effettiva** (`AlertSeverityResolver`, derivata dall'effetto) con `notifications.alerts.severities=SEVERE`, cioè servizio sospeso o fermate non servite, più la regola `notifications.alerts.min-routes-for-push=5`: un avviso che tocca almeno cinque linee riguarda la rete e passa comunque. Misurato il 18/09/2026 su 116 avvisi attivi: 113 sono WARNING (quasi tutti deviazioni per lavori su una linea sola) e 6 toccano cinque o più linee. Le deviazioni della propria linea restano da notificare con i topic per linea e per fermata, che sono un lavoro a sé

### SSE — `VehiclePositionsSseService`
- Push delle posizioni mezzi ai client connessi; set concorrente di emitter con filtri di sottoscrizione (`linea`, `destination`, `vehicleId`)
- Il payload è **serializzato una volta per tick** e condiviso fra i subscriber con filtri identici (nel caso comune, nessun filtro, è la stessa stringa JSON per tutti); il capolinea normalizzato (regex NFD) è precalcolato una volta per veicolo
- L'invio avviene su executor dedicato a **thread singolo** (`vehicle-positions-sse-send`): `emitter.send` è bloccante, e un client lento non deve poter bloccare il polling GTFS-RT. Il thread singolo garantisce anche l'ordine degli eventi per emitter
- Richiede config Nginx specifica (vedi [deploy](../deploy/ARCHITECTURE.md)); è escluso dal filtro ETag, che bufferizzando romperebbe lo streaming

---

## Prestazioni e caching

> Contesto e misure complete in [`ANALISI_PERFORMANCE.md`](../ANALISI_PERFORMANCE.md) (finding B1, B6, B7)
> e §4 di [`ANALISI_API.md`](../ANALISI_API.md).

### Indice di offset su `stop_times.txt` — `GtfsIndexService`
`stop_times.txt` pesa ~245 MB / 5,46M righe. `/trips/{id}/stops` e `/vehicles/{id}/next-stops` lo ri-scandivano **per intero a ogni richiesta** per estrarre le ~20-50 righe di una singola corsa.

Al rebuild si costruisce ora un indice `tripId → byte-range` (scanner byte-level che rispetta virgolette e newline nei campi CSV), sottoposto a swap atomico come gli altri indici. Il lookup legge solo i range del trip via `FileChannel` e li riparsa con lo stesso parser del percorso lento: **da ~1.300 ms a &lt;1 ms**.

L'indice memorizza dimensione e mtime del file: se non combaciano (feed appena sostituito, rebuild non ancora completato) si **ricade sulla scansione completa** invece di leggere byte a caso. Stesso spirito per i guardrail `MAX_TRIP_RANGE_BYTES` / `MAX_TRIP_ID_BYTES`: un range assurdo significa indice incoerente, non richiesta lecita.

### Indici per-data: una sola passata + warm-up
I due indici per giornata di servizio — *"quali corse passano da questa fermata e quando"* (`ScheduledStopIndex`) e *"in che finestra oraria è attiva questa corsa"* (`ActiveTripIndex`) — guardano le stesse righe da due angoli diversi e prima **se le rileggevano a testa**, con due lock separati. Ora nascono da un'unica scansione (`buildPerDateIndexes`) sotto un lock unico, e la cache tiene solo il giorno prima e il giorno dopo (ogni data costa decine di MB).

Il warm-up (`gtfs.index.warmup.*`) li costruisce in background a fine rebuild, sul thread `gtfs-index-warmup`, così il primo utente dopo ogni ricostruzione non paga la scansione sul proprio thread: **da ~2,8 s a ~0,04 s** sulla prima `/nearby` (e ~5,2 s sulla prima `/vehicles?linea=X`).

Dettagli che è bene non riscoprire da soli:

- **`warmup.days=2` non è generosità**: "domani" è la data che `/vehicles?linea=X` interroga di suo quando le corse di oggi non bastano, e quella che gli arrivi usano a cavallo della mezzanotte. Alzare il numero costa una scansione e qualche decina di MB di heap per giorno — sui 768 MB del container di produzione è un ottimo modo per far morire il processo.
- Il warm-up porta con sé la **generazione** del rebuild che l'ha accodato: se nel frattempo ne è arrivato uno più recente, si ferma invece di scaldare indici già invalidati. L'invalidazione avviene sotto lo stesso lock della costruzione, così un warm-up vecchio non può reinserire un indice costruito sul feed precedente.
- Un warm-up fallito degrada la latenza, non la correttezza: la costruzione pigra resta come rete di sicurezza.

Misure sul feed reale (Mac di sviluppo, agosto 2026, dai log `[GTFS-Index]` della suite di test):

| Fase | Tempo |
|---|---|
| Rebuild indici statici (8.285 fermate, 165.585 corse, 428 linee) | ~1.500 ms |
| di cui indice offset `stop_times` | ~800-870 ms |
| Indici per-data, una giornata (~31.000 corse attive) | ~1.500 ms |
| Warm-up completo (2 giorni) | ~3.000 ms |

### ETag + Cache-Control — `config/StaticDataCacheConfig`
Un `ShallowEtagHeaderFilter` ristretto a una whitelist di percorsi, più `Cache-Control: public, max-age=300`.

| Cachato | Escluso (e perché) |
|---|---|
| `/api/v1/catalog/**` | `/api/v1/vehicles/stream` — bufferizzare romperebbe l'SSE |
| `/api/v1/stops` | `/api/v1/vehicles`, `/api/v1/nearby`, `/api/v1/alerts`, `/api/v1/stops/{id}/arrivals` — cambiano ogni 5 s, cacharli li renderebbe sbagliati |
| `/api/v1/stops/search` | |
| `/api/v1/trips/{id}/shape` | |
| `/api/v1/trips/{id}/stops` | |

Effetto: la seconda `/stops` costa un **304 vuoto** invece di 990 KB (241 KB compressi). Il `Cache-Control` non è ridondante rispetto all'ETag: senza, il browser rivalida comunque a ogni richiesta — l'ETag risparmia banda, `max-age` risparmia anche il giro di rete.

> **Trappola da ricordare** (costa ore quando si ripresenta): un ETag calcolato sul corpo della
> risposta funziona solo se il corpo è **stabile a dati invariati**. `ApiListResponseDTO.generatedAt`
> valorizzato con `Instant.now()` cambiava a ogni chiamata, l'ETag cambiava con lui e il 304 non
> arrivava **mai**, pur essendo tutto configurato correttamente. Su `/stops` e `/stops/search`
> `generatedAt` è quindi `GtfsIndexService.dataVersion()`, cioè l'istante dell'ultimo rebuild:
> la *versione dei dati*, non l'istante della richiesta. Chiunque aggiunga un endpoint alla
> whitelist deve controllare che nella risposta non resti nessun campo variabile nel tempo.

---

## Robustezza dei parametri e degli id

### Bean Validation — `ApiNearbyController` + `config/ApiExceptionHandler`
`spring-boot-starter-validation` con vincoli sui `@RequestParam` di `/nearby`, e un `@RestControllerAdvice` che traduce le violazioni in **400 `ProblemDetail`** (senza, una `ConstraintViolationException` arriverebbe al client come 500: un parametro sbagliato è colpa della richiesta, non del server). Il messaggio riporta il nome del parametro, non il path completo del metodo.

> **Sul caso `NaN`.** Prima dei vincoli, `lat=NaN` passava, degenerava nel calcolo della distanza e
> restituiva `200` con fermate arbitrarie: un errore silenzioso, il peggiore. Oggi e' respinto.
>
> Chi lo respinge, verificato sul backend in esecuzione (19/08): **i vincoli stessi**. La risposta a
> `lat=NaN` e' `400` con entrambe le violazioni di intervallo (`deve essere inferiore a o uguale a 90`
> e `superiore a o uguale a -90`), cioe' il messaggio di Hibernate Validator, non quello del controllo
> esplicito. Hibernate tratta `NaN` come fallimento di `@DecimalMin` e `@DecimalMax`, nonostante
> leggendo il codice sembri il contrario (ogni confronto con NaN e' falso). Stesso esito per `Infinity`.
>
> Il `Double.isFinite(...)` in testa al metodo resta comunque, come rete di sicurezza che non dipende
> dai dettagli interni del validatore: se un domani cambiassero, il caso resterebbe coperto. Per un
> nuovo parametro `double` conviene ripetere entrambi.

### 404 e 503 — `config/ResourceNotFoundException`
`404` quando la risorsa **non esiste** nel feed caricato: `/stops/{id}/arrivals` (prima: `200` con il segnaposto finto `"Fermata non trovata"`) e `/trips/{id}/stops` (prima: `200 []`). Una risorsa che esiste ma **non ha dati adesso** resta `200` con lista vuota: sono due situazioni diverse e il client deve poterle distinguere — la prima è un id sbagliato o obsoleto, la seconda è semplicemente notte fonda.

> **Perché 503 e non 404 quando gli indici sono vuoti.** Il refresh di startup gira in background:
> per qualche decina di secondi il server accetta richieste con il catalogo a zero, e in quella
> finestra *nessun* id esiste. Rispondere 404 sarebbe una bugia con una conseguenza concreta: un
> client che ripulisce i preferiti sui 404 — comportamento del tutto ragionevole, ed è esattamente
> l'uso per cui il 404 è stato introdotto — **cancellerebbe fermate perfettamente valide**, in modo
> irreversibile e senza che l'utente capisca perché. `GtfsIndexService.isStaticDataLoaded()` fa da
> guardia e il 503 dice la verità: "riprova fra poco", non "non esiste".

Copertura attuale: la guardia c'è su `/stops/{id}/arrivals` e `/trips/{id}/stops`. **Non** su `/trips/{id}/shape` né su `/vehicles/{id}/next-stops`, che continuano a rispondere `200` con contenuto vuoto.

---

## Integrazioni esterne

- **OpenTripPlanner** (GraphQL su 8081, container Docker `otp`) — planner multimodale
- **Roma Mobilità / ATAC** — feed GTFS statico + 3 GTFS-RT
- **Trenitalia ViaggiaTreno** — `viaggiatreno.it`
- **Nominatim-like / Photon** — geocoding via `ReverseGeocodeService` / `GeocodeSearchService`
- **Firebase Cloud Messaging** — push su topic (opzionale, degradazione pulita senza credenziali)
- **MIT** — RSS scioperi `scioperi.mit.gov.it`

---

## Build & run

### Build jar
```bash
./mvnw -DskipTests package
cp target/*.jar target/gtfs-monitor.jar
```

### Docker
[`Dockerfile`](Dockerfile):
```dockerfile
FROM eclipse-temurin:21-jre
COPY app.jar /app/app.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/app.jar"]
```

### Compose locale ([`docker-compose.yml`](docker-compose.yml))
- profilo `local`, GTFS in `./data/gtfs_static`, OTP `localhost:8081`
- volumi: `./app.jar`, `./data/gtfs_static`, `./logs`
- servizio `postgres` per lo storico, dati in `./data/postgres`, password in `.env` (`POSTGRES_PASSWORD`)
- heap default 512m–1g

### Profilo prod
Vedi [deploy/docker-compose.prod.yml](../deploy/docker-compose.prod.yml) per il setup completo BE + OTP + Nginx.

---

## Test

- Test unit/integration in [`src/test/java`](src/test/java)
- Postman collection: [`gtfs-monitor-local-tests.postman_collection.json`](gtfs-monitor-local-tests.postman_collection.json)
- Script di profiling: [`scripts/profile-api.sh`](scripts/profile-api.sh) (`BASE_URL`, `RUNS`, `WARMUP`, `DELAY_MS`; `STOP_ID`/`TRIP_ID`/`TRAIN_NUMBER` per includere gli endpoint parametrici)

| Classe | Cosa copre |
|---|---|
| `AlertSeverityResolverTest` | mappatura effetto → severità, `FEED` vs `DERIVED`, `UNKNOWN_SEVERITY` |
| `GtfsIndexServiceScheduledStopsTest` | caratterizzazione di `scheduledStops` / `scheduledNextStops` su feed sintetico |
| `GtfsIndexServiceStopTimesPerfTest` | **opt-in**: equivalenza percorso veloce/lento e misura sul feed reale in `data/gtfs_static`. Si abilita con `./mvnw test -Dtest=GtfsIndexServiceStopTimesPerfTest -Dgtfs.stopTimesPerf=true`, altrimenti risulta *skipped* |
| `VehiclePositionsSseServiceTest` | simulazione veicoli e filtri di sottoscrizione SSE |
| `GtfsMonitorApplicationTests` | `contextLoads` |

### Tre test rossi, per motivi preesistenti

`./mvnw test` oggi chiude con **16 test, 2 failure + 1 error + 1 skipped**. Nessuno dei tre dipende dalle modifiche di agosto: se li trovate rossi appena clonato il progetto, **non avete rotto niente**.

| Test | Sintomo | Causa |
|---|---|---|
| `VehiclePositionsSseServiceTest.simulateVehiclesKeepsReturningPredicted990LForReportedStreamInstant` | `expected: <false> but was: <true>` | **data hardcoded** `2026-03-07T19:14:06.952Z`. Entrambi i test costruiscono l'indice sul feed reale in `data/gtfs_static`, che oggi copre 05/08→30/09/2026: in quella data non c'è nessun servizio attivo e la lista torna vuota |
| `VehiclePositionsSseServiceTest.filterSnapshotKeepsSpecificPredictedVehicleAliveForSimulatedVehicleSubscriptions` | idem | stessa causa, stesso istante hardcoded |
| `GtfsMonitorApplicationTests.contextLoads` | `NullPointerException` in `Path.of` durante l'init di `gtfsIndexService` | `gtfs.static-props.data-dir` è definito **solo** nei profili `local`/`prod`; il test gira senza profilo e riceve `null` |

Come si chiuderebbero, se e quando si decide di farlo: i primi due iniettando l'istante da un feed di prova invece di fissarlo nel codice; il terzo con un `application-test.properties` (oggi `src/test/resources/` non esiste) o `@SpringBootTest(properties = "gtfs.static-props.data-dir=...")` su una directory temporanea.

---

## Note di design

### Dati e concorrenza
- Realtime cachato in memoria con `AtomicReference` thread-safe; gli indici statici fanno swap atomico, il reindex **non** blocca le letture
- Static GTFS indicizzato in memoria; supporta `calendar_dates` per validità trip
- `GtfsIndexService.dataVersion()` è la versione dei dati statici, non un timestamp di comodo: la usano gli ETag
- Nel costruire gli indici per-data si riusa l'istanza di `tripId` già presente in `trips.txt` invece della String nuova che il parser crea per riga: sono ~940.000 righe per data, cioè un oggetto invece di un milione
- Risposte API standardizzate con `ApiListResponseDTO` (`items`, `total`, `generatedAt`)
- Tutto il temporale in `ZoneId.of("Europe/Rome")`

### Il feed statico all'avvio: stato ripreso, indici dal disco
`StaticGtfsUpdater` scriveva `.gtfs_static_state.json` a ogni refresh ma non lo rileggeva mai. Conseguenza: dopo ogni riavvio `etag` e `lastModified` ripartivano da `null`, la richiesta condizionale non poteva essere inviata e il feed veniva **riscaricato per intero — una sessantina di MB — anche quando non era cambiato di una riga**.

Rileggerlo (`@PostConstruct riprendiStato()`) ha però scoperto un secondo problema, che il primo teneva nascosto: gli indici venivano costruiti come effetto collaterale dell'estrazione dello zip. Con la richiesta condizionale funzionante il refresh d'avvio riceve un **304**, non c'è nessuno zip da estrarre, e l'applicazione partiva con il catalogo vuoto — `/catalog/lines` rispondeva `[]`.

Da qui la regola: **gli indici si costruiscono da ciò che c'è sul disco, il download serve ad aggiornare il disco.** All'avvio, se `stop_times.txt` esiste, `costruisciIndiciDaDisco()` li ricostruisce prima ancora di sentire la rete; il refresh che segue aggiorna soltanto.

Sequenza attesa nei log di un avvio con feed già aggiornato:

```
[StaticGTFS] Stato ripreso dal disco: Last-Modified=Thu, 27 Aug 2026 05:33:59 GMT
[StaticGTFS] Indici costruiti dal feed gia' presente sul disco
[StaticGTFS] Ricevuta risposta HTTP 304 NOT_MODIFIED
[StaticGTFS] Nessun aggiornamento (304 Not Modified)
```

### Avvisi: severità derivata, e perché la distinzione conta
Il feed GTFS-RT di Roma Mobilità **non popola mai** `severity_level`: misurato il 19/08/2026, `severity` era `null` su tutti i 115 avvisi attivi. Le varianti *warning* e *critical* della UI non comparivano quindi mai, e un servizio sospeso pesava visivamente come una nota informativa. `effect`, invece, il feed lo popola: `AlertSeverityResolver` ne ricava una severità (110 warning, 4 info, 1 severe sugli stessi 115 avvisi).

Il criterio della mappatura è **l'effetto sul viaggio di chi legge**, non la gravità dell'evento che l'ha causato: `NO_SERVICE`/`NO_STOPS` → `SEVERE` (la corsa non c'è o non ferma dove serve, bisogna cambiare piano); `DETOUR`, `MODIFIED_SERVICE`, `REDUCED_SERVICE`, `SIGNIFICANT_DELAYS`, `STOP_MOVED`, `ACCESSIBILITY_ISSUE` → `WARNING`; tutto il resto, incluso l'ignoto, → `INFO`. In dubbio non si allarma.

Tre scelte che sembrano dettagli e non lo sono:

1. **`severitySource` + `declaredSeverity` esistono per non spacciare un'inferenza per un dato.** Il client (o chi legge un log fra sei mesi) deve poter distinguere "il feed dice che è grave" da "l'abbiamo dedotto noi dall'effetto". Senza questa distinzione, il giorno in cui Roma Mobilità inizierà davvero a popolare `severity_level` nessuno saprà dire quali valori storici erano reali; e una derivazione sbagliata diventerebbe indistinguibile da un errore del feed, con il debug che parte dalla parte sbagliata. Il vocabolario in uscita è comunque quello di GTFS-RT (`SeverityLevel`), lo stesso che uscirebbe dal feed: il contratto verso i client non cambierà.
2. **La derivazione parte da `effectCode`, non dall'etichetta italiana.** Ritoccare una traduzione in `ServiceAlertsService.EFFECT_IT` non deve poter spostare in silenzio la gravità di un avviso. Per lo stesso motivo `causeCode`/`effectCode` sono esposti ai client: le etichette servono a chi legge, i codici a chi ragiona.
3. **Il filtro delle push continua a leggere la severità *dichiarata*, non quella derivata** (`notifications.alerts.severities=SEVERE,WARNING` in `ServiceAlertsService`). Oggi questo significa che il filtro blocca tutto, ed è voluto: con la derivata diventerebbero notificabili **112 avvisi su 115**, quasi tutti deviazioni per lavori. Spam. L'ambito della derivazione è deliberatamente limitato all'API di lettura; allargare le push è una decisione separata, con criteri suoi. Chi in futuro "uniformerà le due logiche" per coerenza dovrebbe leggere prima questa nota.

### Multi-linea negli avvisi
`ServiceAlertDTO.routeIds` è sempre stato plurale e corretto: era l'esposizione a valle che appiattiva su `routeIds.getFirst()`. Sui dati reali del 19/08, **21 avvisi su 115 riguardano più di una linea** (casi peggiori: 12 linee per i cantieri di Piazza Venezia) e l'appiattimento perdeva **62 associazioni linea↔avviso su 83, il 75%**. Conseguenza pratica: una fermata servita dalla 64, o un itinerario che la usa, non mostrava l'avviso dei cantieri perché quell'avviso risultava della C3 — mentre la pagina Avvisi, che filtra sul `routeIds` completo lato server, lo mostrava. Due viste incoerenti sullo stesso dato.

`line` resta come primo elemento di `lines` solo per la transizione: **ogni logica nuova deve indicizzare su `lines`**. Dettagli e misura in §1 di [`ANALISI_API.md`](../ANALISI_API.md).

### Contratto API
Il client web contiene ancora uno strato di mappatura difensiva che indovina fra `titolo`/`headerText`/`title` e fra `severita`/`severity`/`severityLevel` (§3 di [`ANALISI_API.md`](../ANALISI_API.md)). I campi aggiunti ad agosto vanno nella direzione opposta — nomi in inglese, coerenti col resto dell'API — ma finché i fallback restano lato client i disallineamenti continuano a essere nascosti invece che emergere.
