package it.roma.gtfs.gtfs_monitor.model.dto;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class VehiclePositionDTO {
    String linea;        // route_id
    String corsa;        // trip_id
    String veicolo;      // vehicle.id
    Double lat;          // position.latitude
    Double lon;          // position.longitude
    Double velocitaKmh;  // position.speed (m/s) -> km/h
    String timestamp;    // ISO Europe/Rome
    String capolinea;    // trip_headsign da trips.txt

}