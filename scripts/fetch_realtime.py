from datetime import datetime
import requests
import os
import logging
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from dotenv import load_dotenv
from file_saver import FileSaver
from abc import ABC, abstractmethod
from utils import LogObfuscator

load_dotenv()


class RealtimeFetcher(ABC):
    def __init__(self, endpoint, now, raw_dir, clean_dir, timeout=5, session=None):
        self._init_logger(now)
        self.session = session or requests.Session()
        self.endpoint = endpoint
        self.timeout = timeout

        self.raw_file_saver = FileSaver(raw_dir, now)
        self.clean_file_saver = FileSaver(clean_dir, now)

    def _init_logger(self, now):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(f"data/runs/realtime_{now}.log")
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        fh.setFormatter(formatter)
        ch = logging.StreamHandler()
        ch.addFilter(LogObfuscator([os.getenv("TRANSLINK_API_KEY")]))
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    @abstractmethod
    def to_clean_dict(self, feed):
        pass

    @abstractmethod
    def save_raw(self, feed):
        pass

    @abstractmethod
    def save_clean(self, feed):
        pass

    def fetch_raw(self):
        try:
            self.logger.info(f"Fetching from f{self.endpoint}")
            r = self.session.get(self.endpoint, timeout=self.timeout)
            r.raise_for_status()
            self.logger.info("Fetch successful.")
            return r.content
        except requests.exceptions.Timeout:
            self.logger.error("Timeout occurred while fetching feed.", exc_info=True)
            raise
        except requests.exceptions.HTTPError as e:
            self.logger.error(
                f"HTTP error {e.response.status_code} while fetching feed: {e}",
                exc_info=True,
            )
            raise
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error: {e}", exc_info=True)
            raise
        except Exception as e:
            self.logger.exception(f"Unexpected error while fetching feed: {e}")
            raise

    def parse(self, raw_bytes):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw_bytes)
        return feed

    def to_raw_dict(self, feed):
        feed_dict = MessageToDict(feed)
        return feed_dict

    def run(self):
        raw = self.fetch_raw()
        feed = self.parse(raw)
        self.save_raw(feed)
        self.save_clean(feed)


class PositionsFetcher(RealtimeFetcher):

    def __init__(self, session, now=None, timeout=5):
        endpoint = f"https://gtfsapi.translink.ca/v3/gtfsposition?apikey={os.getenv('TRANSLINK_API_KEY')}"
        super().__init__(
            endpoint,
            raw_dir="data/raw/realtime/position_updates",
            clean_dir="data/clean/realtime/position_updates",
            now=now,
            timeout=timeout,
            session=session,
        )

    def to_clean_dict(self, feed):
        positions = []
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue

            v = entity.vehicle

            positions.append(
                {
                    "vehicle_id": v.vehicle.id or None,
                    "trip_id": v.trip.trip_id or None,
                    "route_id": v.trip.route_id or None,
                    "lat": v.position.latitude if v.HasField("position") else None,
                    "lon": v.position.longitude if v.HasField("position") else None,
                    "timestamp": v.timestamp if v.timestamp else None,
                }
            )
        return positions

    def save_raw(self, feed):
        self.raw_file_saver.save_json("position_updates", self.to_raw_dict(feed))

    def save_clean(self, feed):
        self.clean_file_saver.save_json("position_updates", self.to_clean_dict(feed))


class TripUpdatesFetcher(RealtimeFetcher):

    def __init__(self, session, now=None, timeout=5):
        endpoint = f"https://gtfsapi.translink.ca/v3/gtfsrealtime?apikey={os.getenv('TRANSLINK_API_KEY')}"
        super().__init__(
            endpoint,
            raw_dir="data/raw/realtime/trip_updates",
            clean_dir="data/clean/realtime/trip_updates",
            now=now,
            timeout=timeout,
            session=session,
        )

    def to_clean_dict(self, feed):
        trips = []
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            tu = entity.trip_update
            stop_updates = []
            for s in tu.stop_time_update:
                stop_updates.append(
                    {
                        "stop_id": s.stop_id,
                        "arrival": s.arrival.time if s.HasField("arrival") else None,
                        "departure": (
                            s.departure.time if s.HasField("departure") else None
                        ),
                        "arrival_delay": (
                            s.arrival.delay if s.HasField("arrival") else None
                        ),
                        "departure_delay": (
                            s.departure.delay if s.HasField("departure") else None
                        ),
                    }
                )
            trips.append(
                {
                    "trip_id": tu.trip.trip_id,
                    "route_id": tu.trip.route_id,
                    "stop_time_updates": stop_updates,
                }
            )
        return trips

    def save_raw(self, feed):
        self.raw_file_saver.save_json("trip_updates", self.to_raw_dict(feed))

    def save_clean(self, feed):
        self.clean_file_saver.save_json("trip_updates", self.to_clean_dict(feed))


class AlertsFetcher(RealtimeFetcher):

    def __init__(self, session, now=None, timeout=5):
        endpoint = f"https://gtfsapi.translink.ca/v3/gtfsalerts?apikey={os.getenv('TRANSLINK_API_KEY')}"
        super().__init__(
            endpoint,
            raw_dir="data/raw/realtime/service_alerts",
            clean_dir="data/clean/realtime/service_alerts",
            now=now,
            timeout=timeout,
            session=session,
        )

    def to_clean_dict(self, feed):
        alerts = []
        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue

            alert = entity.alert

            informed = []
            for i in alert.informed_entity:
                informed.append(
                    {
                        "trip_id": i.trip.trip_id or None,
                        "route_id": i.route_id or None,
                        "stop_id": i.stop_id or None,
                    }
                )

            alerts.append(
                {
                    "cause": gtfs_realtime_pb2.Alert.Cause.Name(alert.cause),
                    "effect": gtfs_realtime_pb2.Alert.Effect.Name(alert.effect),
                    "header": (
                        alert.header_text.translation[0].text
                        if alert.header_text.translation
                        else ""
                    ),
                    "description": (
                        alert.description_text.translation[0].text
                        if alert.description_text.translation
                        else ""
                    ),
                    "informed_entities": informed,
                }
            )

        return alerts

    def save_raw(self, feed):
        self.raw_file_saver.save_json("service_alerts", self.to_raw_dict(feed))

    def save_clean(self, feed):
        self.clean_file_saver.save_json("service_alerts", self.to_clean_dict(feed))


def init_logging(now):
    logger = logging.getLogger("Realtime")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"data/runs/realtime_{now}.log")
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


def run(timestamp=None):
    now = timestamp or datetime.now().strftime("%Y-%m-%dT%H-%M")
    logger = init_logging(now)

    session = requests.Session()

    fetchers = [
        PositionsFetcher(session=session, now=now),
        TripUpdatesFetcher(session=session, now=now),
        AlertsFetcher(session=session, now=now),
    ]

    for f in fetchers:
        logger.info(f"Running {f.__class__.__name__}")
        f.run()

    logger.info("Run complete.")


if __name__ == "__main__":
    run()
