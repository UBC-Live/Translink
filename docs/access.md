# Translink API

https://www.translink.ca/about-us/doing-business-with-translink/app-developer-resources/gtfs/gtfs-realtime

https://www.translink.ca/about-us/doing-business-with-translink/app-developer-resources/gtfs/gtfs-data

## TOS

- Translink allows up to 1000 requests per day.
- Must display the following disclaimer
  > "Some of the data used in this product or service is provided by permission of TransLink. TransLink assumes no responsibility for the accuracy or currency of the Data used in this product or service."

## Static Data

Example GTFS format: https://gtfs.org/getting-started/example-feed/

- Returns data like bus stops, transit lines, etc. in CSV format.
- Can be used to understand realtime data
  - routes.csv: route_id → route_short_name (bus number) → route_long_name (bus line name)
  - stops.csv: stop_id / stop_code → stop_name/stop_lat/stop_lon
  - stop_times: trip_id → arrival/departure time
- Vehicle position entity mapped by tripId/routeId

## Realtime data

- Trip updates
  - delays, cancellations, changed routes
  - `https://gtfsapi.translink.ca/v3/gtfsrealtime?apikey=[API_KEY]`
- Service alerts
  - stops moved, unforseen events
  - `https://gtfsapi.translink.ca/v3/gtfsalerts?apikey=[API_KEY]`
- Vehicle Positions / Position Update
  - vehicle information including location and congestion level
  - `https://gtfsapi.translink.ca/v3/gtfsposition?apikey=[API_KEY]`

### Protocol Buffers

- Realtime data is returned in protocol buffer format (.pb)
- Parse into JSON File

  ```python
  import json
  from google.transit import gtfs_realtime_pb2
  from google.protobuf.json_format import MessageToDict

  with open("test.pb", "rb") as f:
      data = f.read()

  feed = gtfs_realtime_pb2.FeedMessage()
  feed.ParseFromString(data)

  feed_dict = MessageToDict(feed)
  with open("feed.json", "w") as out:
      json.dump(feed_dict, out, indent=2)

  ```

```python
 // EXAMPLE VEHICLE POSITION

 "header": {
    "gtfsRealtimeVersion": "2.0",
    "incrementality": "FULL_DATASET",
    "timestamp": "1763288261"
  },
    "entity": [
    {
      "id": "14731511",
      "vehicle": {
        "trip": {
          "tripId": "14731511",
          "startDate": "20251115",
          "scheduleRelationship": "SCHEDULED",
          "routeId": "6836",
          "directionId": 0
        },
        "position": {
          "latitude": 49.2637,
          "longitude": -123.16814
        },
        "currentStopSequence": 15,
        "currentStatus": "IN_TRANSIT_TO",
        "timestamp": "1763288219",
        "stopId": "59",
        "vehicle": {
          "id": "8137",
          "label": "8137"
        }
      }
    },
    ...
    ],
```
