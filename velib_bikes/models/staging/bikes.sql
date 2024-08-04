select
  stationcode as station_id,
  name as station_name,
  capacity as capacity, 
  numdocksavailable as number_of_available_docks,
  numbikesavailable as number_of_available_bikes,
  mechanical as mechanical,
  ebike as electronic_bike, 
  duedate as due_data,
  is_renting as is_renting,
  is_returning

from {{source('velib_bikes', 'bikes_availibilities4')}}
