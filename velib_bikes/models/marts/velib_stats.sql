select 
  station_id, 
  station_name,
  count(number_of_available_docks) as available_docks,
  count(number_of_available_bikes) as available_bikes
from {{ref('bikes')}}
group by station_id, station_name
order by available_docks desc
