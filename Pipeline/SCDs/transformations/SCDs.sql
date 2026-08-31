CREATE OR REFRESH STREAMING TABLE workspace.enr.DimAirports;

CREATE FLOW flow1
AS AUTO CDC INTO
 DimAirports
FROM stream(stg.stream_airports)
 KEYS (airport_id)
 SEQUENCE BY airport_id
 STORED AS SCD TYPE 2;

 CREATE OR REFRESH STREAMING TABLE workspace.enr.DimPassengers;

CREATE FLOW flow2
AS AUTO CDC INTO
 DimPassengers
FROM stream(stg.stream_passengers)
 KEYS (passenger_id)
 SEQUENCE BY passenger_id
 STORED AS SCD TYPE 2;
