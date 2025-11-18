## Description:
This repository is responsible for TransLink related tasks for UBC Live 

## Data format: 
- Data is retrieved from TransLink's GTFS Static and GTFS-Realtime APIs. 
- Static data (routes, stops, stop times, trips) is provided in CSV format.
- Realtime data (vehicle positions, trip updates. service alerts) is provided in Protocol Buffer (.pb) format which is coverted into JSON. 

## Data format (CSV):
### Header definitions
**route_number**
Public bus route number derived from GTFS static 
```csv
"99
```
**vehicle_id**
Unique identifier for the bus reporting real-time data.
```csv
"8137"
```
**latitude, longitude**
Current GPS coordinates of the bus from the realtime feed.
```csv
49.2637,-123.16814
```
**timestamp**
The UNIX timestamp when the update was recorded.
Converted to ISO-8601 for consistency.
```csv
"2025-11-15T23:45:02Z"
```
**stop_id**
ID of the upcoming stop the bus is traveling toward.
Comes directly from stopId in the vehicle position entity.
```csv
"59"
```
**current_stop_sequence**
Index of the stop along the trip pattern according to GTFS-Realtime.
```csv
15
```
**arrival_estimate** (derived)
Estimated arrival time (in minutes) until the vehicle reaches stop_id.
Computed by our pipeline (TransLink does not provide this directly).
```csv
3
```
**occupancy**
Crowding level of the bus.
Only present when TransLink includes congestionLevel.
```pb
"FULL"
"MANY_SEATS"
"EMPTY"
```
### Example

```csv
route_number,vehicle_id,latitude,longitude,timestamp,stop_id,current_stop_sequence,arrival_estimate,occupancy
"99","8137",49.2637,-123.16814,"2025-11-15T23:45:02Z","59",15,3,"MANY_SEATS"
```

- Raw data will be in `data/raw/` and cleaned data in `data/raw/clean/`. 
- **Note:** The format of the data or CSV file may be changed as requirements evolve

## Setup Instructions 

1. **Clone the repository**
   ```bash
   git clone https://github.com/UBC-Live/Translink.git
   cd Translink
   ```

2. **Create virtual environment** 
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
   
   To deactivate:
   ```bash
   deactivate
   ```

3. **Set up environment variables**
   Create a copy .env.example contents into a new .env file, then put in API information.
   Copy .env.example contents into a new .env file, then put in API information. **This file is local and should not be pushed, as specified in .gitignore** 

4. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```