# gtfs-monitor — Backend Spring Boot

Backend REST + SSE della piattaforma **WeMoveRoma**. Si occupa di ingerire i feed GTFS (statico e realtime) di ATAC / Roma Mobilità, esporli via API versionata `/api/v1`, fare da proxy verso OpenTripPlanner per il planner multimodale e gestire integrazioni accessorie (Trenitalia, geocoding).

> Vedi anche: [workspace overview](../ARCHITECTURE.md) — [deploy](../deploy/ARCHITECTURE.md) — [OTP](../otp/ARCHITECTURE.md)

---

## Stack

- **Spring Boot 3.5.7** su **Java 21**
- Build: **Maven** (`./mvnw package` → `target/*.jar` → `app.jar`)
- Starter: `web` (MVC), `webflux` (WebClient reattivo), `actuator`, `cache`
- **gtfs-realtime-bindings 0.0.8** + `protobuf-java 3.25.5` per il parsing dei feed PB
- **univocity-parsers 2.9.1** per il GTFS statico (CSV)
- Lombok, Jackson JSR310

---

## Configurazione

File principali in [`src/main/resources/`](src/main/resources):

- `application.properties` — base
- `application-local.properties` — dev locale (CORS `localhost:4200/4203/5173`, GTFS in `data/gtfs_static`, OTP `http://localhost:8081`)
- `application-prod.properties` — produzione (CORS `wemoveroma.com`, OTP `http://otp:8081` via rete Docker)

### Chiavi rilevanti

| Gruppo | Chiave | Note |
|--------|--------|------|
| GTFS statico | `gtfs.static-props.url` | feed Roma Mobilità, refresh 1h |
| GTFS realtime | `gtfs.realtime.{trip-updates,vehicle-positions,service-alerts}-url` | polling 5s |
| OTP | `journey.otp.enabled`, `journey.otp.base-url`, `journey.otp.search-window(-fallback)`, `journey.otp.max-itineraries` | search 90–180 min; il tuning a piedi/salita è in `otp/data/router-config.json`, non più nella query |
| Planner treno | `journey.otp.rail-visibility-slot` (2), `rail-visibility-max-delay-minutes` (45), `rail-injection-enabled` (true), `rail-injection-max-station-meters` (1200) | visibilità/iniezione opzione treno (vedi sezione Journey planner) |
| Rail | `rail.viaggiatreno.*` | integrazione Trenitalia (ViaggiaTreno, dati live) |
| Web | `spring.codec.max-in-memory-size=104MB` | richiesto per protobuf grandi |
| CORS | `app.cors.allowed-origins` | per profilo |

---

## Struttura dei package

```
src/main/java/.../gtfsmonitor/
├── controller/   ← 15 REST controller @RestController (vedi sotto)
├── service/      ← logica: indici GTFS, polling RT, SSE, planner, geocoding, alert, rail
├── model/dto/    ← 25+ DTO per le risposte API
├── config/       ← WebClient, binding properties GTFS, CORS
└── utils/        ← util varie (es. DelayFmt)
```

---

## REST API (`/api/v1`)

### Vehicles — `ApiVehiclesController`
- `GET /vehicles` — lista mezzi (filtri: `linea`, `destination`, `limit`)
- `GET /vehicles/stream` — **SSE** stream posizioni in tempo reale
- `GET /vehicles/{vehicleId}/next-stops` — prossime fermate (scheduled + actual)

### Stops — `ApiStopsController`
- `GET /stops` — lista (bbox o totale)
- `GET /stops/search` — ricerca per nome
- `GET /stops/{stopId}/arrivals` — arrivi previsti

### Journey planner — `ApiJourneyController`
- `GET /journey/plan` — pianificazione multimodale via OTP (`fromLat`, `fromLon`, `toLat`, `toLon`, `numItineraries`, `timeMode`, `when`, `modes`)

Il `JourneyPlannerService` interroga OTP (GraphQL `planConnection`) e poi **post-processa** i risultati:

- **Dedup + ranking** (`dedupeAndEnrich`): raggruppa itinerari con lo stesso pattern (linee/fermate), ordina con camminata-pura in fondo e poi per orario di arrivo; le corse equivalenti diventano `alternativeBoardingTimes`.
- **Visibilità treno** (`promoteBestRailOption`): garantisce che la migliore opzione con leg `RAIL` compaia entro `journey.otp.rail-visibility-slot` (senza scalzare la #0), se competitiva entro `rail-visibility-max-delay-minutes`.
- **Iniezione treno** (`tryBuildInjectedRailOption`): workaround per un caso limite di routing access/egress di OTP sui salti brevi (1 fermata). Quando il piano **non** contiene treni ma partenza e arrivo sono entrambi entro `rail-injection-max-station-meters` da una stazione, costruisce un'opzione sintetica *cammino → treno (stazione→stazione) → cammino*: trova le stazioni con `stopsByRadius`, pianifica il treno fra i due centri stazione, sceglie la prima corsa **realmente prendibile** (`when + cammino d'accesso`) e la inietta. Disattivabile con `journey.otp.rail-injection-enabled=false`.

> **Tuning di routing OTP:** velocità/riluttanza a piedi, costo di salita, slack, ecc. **non** viaggiano più nel blocco `preferences` per-richiesta (OTP 2.8.1 lo rifiuta) — la query del backend è senza `preferences`. Il tuning vive in [`otp/data/router-config.json`](../otp/ARCHITECTURE.md) (`routingDefaults`). Restano per-richiesta solo `searchWindow`, `dateTime`, `modes`, `first`.

### Trips — `ApiTripsController`
- `GET /trips/{tripId}/shape` — geometria polyline
- `GET /trips/{tripId}/stops` — fermate schedulate

### Live focus su fermata — `ApiPlannerController`
- `GET /planner/live-stop-focus` — ETA real-time + stato servizio

### Nearby — `ApiNearbyController`
- `GET /nearby?lat=&lon=&radius=` — fermate vicine + arrivi

### Search / Catalog
- `GET /search/suggestions` — autocomplete linee/fermate/indirizzi
- `GET /catalog/lines` — linee disponibili
- `GET /catalog/destinations` — destinazioni di una linea

### Alerts — `ApiAlertsController`
- `GET /alerts` — alert servizio (filtri: `linea`, `active`)

### Geocode — `ApiGeocodeController`
- `GET /geocode/search` — forward (indirizzo → coord)
- `GET /geocode/reverse` — reverse (coord → indirizzo)

### Rail — `ApiRailController`
- `GET /rail/train-info` — stato treno Trenitalia (`trainNumber`, `stationName`, `referenceTime`)

### Dashboard — `ApiDashboardController`
- `GET /dashboard/summary` — metriche aggregate

---

## Background jobs e SSE

### Polling GTFS Realtime
Tutti i servizi `VehiclePositionsService`, `TripUpdatesService`, `ServiceAlertsService` usano `@Scheduled` con `fixedDelay=5s` e mantengono lo stato in `AtomicReference` con lock di refresh.

### Static GTFS — `StaticGtfsUpdater`
- `@EventListener(ApplicationReadyEvent)` — fetch all'avvio
- `@Scheduled(cron="0 40 6,20 * * *", zone="Europe/Rome")` — refresh giornaliero 06:40 e 20:00
- `@Scheduled(fixedDelay=5min)` — retry su errore

### SSE — `VehiclePositionsSseService`
- Push delle posizioni mezzi ai client connessi
- Set concorrente di emitter, filtri sottoscrizione (`linea`, `destination`, `vehicleId`)
- Richiede config Nginx specifica (vedi [deploy](../deploy/ARCHITECTURE.md))

---

## Integrazioni esterne

- **OpenTripPlanner** (GraphQL su 8081, container Docker `otp`) — planner multimodale
- **Roma Mobilità / ATAC** — feed GTFS statico + 3 GTFS-RT
- **Trenitalia ViaggiaTreno** — `viaggiatreno.it`
- **Nominatim-like** — geocoding via `ReverseGeocodeService` / `GeocodeSearchService`

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
- heap default 512m–1g

### Profilo prod
Vedi [deploy/docker-compose.prod.yml](../deploy/docker-compose.prod.yml) per il setup completo BE + OTP + Nginx.

---

## Test

- Test unit/integration in [`src/test/java`](src/test/java)
- Postman collection: [`gtfs-monitor-local-tests.postman_collection.json`](gtfs-monitor-local-tests.postman_collection.json)
- Script di profiling: [`scripts/profile-api.sh`](scripts/profile-api.sh)

---

## Note di design

- Realtime cachato in memoria con `AtomicReference` thread-safe
- Static GTFS indicizzato in memoria; supporta `calendar_dates` per validità trip
- Risposte API standardizzate con `ApiListResponseDTO` (paginazione)
- Tutto il temporale in `ZoneId.of("Europe/Rome")`
