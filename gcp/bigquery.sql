

CREATE SCHEMA `project_id.velib_bikes`
  OPTIONS (
    description= "dataset to organize different tables related to velib bikes",
    location = "us-central1"
  );

CREATE TABLE velib_bikes.bikes_availibilities4 (
  total_count int64 OPTIONS (description = 'Total count of the entries'),
  stationcode string OPTIONS (description = 'Station code id '),
  name STRING OPTIONS (description = 'Name of the bike'),
  is_installed STRING OPTIONS (description = 'Bike is installed '),
  capacity INT64 OPTIONS (description = 'Capacity of the station'),
  numdocksavailable INT64 OPTIONS (description = 'Number of available docks'),
  numbikesavailable INT64 OPTIONS (description = 'Number of available bikes'),
  mechanical INT64 OPTIONS (description = 'Machnical'),
  ebike INT64 OPTIONS (description = 'E-bike'),
  is_renting STRING OPTIONS (description = 'Bike is being rented'),
  is_returning STRING OPTIONS (description = 'Bike is being returned'),
  duedate STRING OPTIONS (description = 'Bike renting due date'),
  logitude FLOAT64 OPTIONS (description = 'Longititude'),
  latitude FLOAT64 OPTIONS (description = 'Latitude'),
  nom_arrondissement_communes STRING OPTIONS (description = 'nom_arrondissement_communes'),
  code_insee_commune STRING OPTIONS (description = 'code_insee_commune'),
  timestamp TIMESTAMP OPTIONS (description = 'Message timestamp')

) 
PARTITION BY _PARTITIONDATE
OPTIONS (
    expiration_timestamp = TIMESTAMP '2025-01-01 00:00:00 UTC',
    description = 'a table that expires in 2025')
    
