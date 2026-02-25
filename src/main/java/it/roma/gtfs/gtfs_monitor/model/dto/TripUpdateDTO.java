package it.roma.gtfs.gtfs_monitor.model.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TripUpdateDTO {
    private String linea;               // route_id
    private String corsa;               // trip_id
    private String veicolo;             // vehicle.id
    private String fermataId;           // stop_id
    private String fermataNome;         // stop_name
    private String arrivo;              // ISO Rome
    private String partenza;            // ISO Rome
    private Double ritardoArrivoMin;    // delay in minuti
    private Double ritardoPartenzaMin;  // delay in minuti
    private String descPartenza;
    private String descArrivo;
}