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
    """
    Abstract base class for fetching, parsing, and saving GTFS-Realtime feeds.

    This class provides the common workflow:
    - fetch raw bytes from a remote endpoint
    - parse the protobuf feed
    - save raw JSON
    - save cleaned/normalized JSON

    Subclasses must implement: `to_clean_dict`, `save_raw`, and `save_clean`.

    Args:
        endpoint (str): URL to fetch the realtime feed from.
        now (str): Timestamp string used to name logs and output folders.
        raw_dir (str): Directory path for saving raw feed JSON.
        clean_dir (str): Directory path for saving cleaned feed JSON.
        timeout (int): HTTP request timeout in seconds.
        session (requests.Session): Optional session to reuse for requests.
    """

    def __init__(self, endpoint, now, raw_dir, clean_dir, timeout=5, session=None):
        self._init_logger(now)
        self.session = session or requests.Session()
        self.endpoint = endpoint
        self.timeout = timeout

        self.raw_file_saver = FileSaver(raw_dir, now)
        self.clean_file_saver = FileSaver(clean_dir, now)

    def _init_logger(self, now):
        """
        Initialize a logger specific to this fetcher instance.

        Creates a file logger and a console logger, with optional API-key
        obfuscation applied through LogObfuscator.

        Args:
            now (str): Timestamp string used to name the log file.
        """
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
        """
        Convert a parsed GTFS feed into a cleaned, normalized dictionary.

        Args:
            feed (gtfs_realtime_pb2.FeedMessage): Parsed protobuf feed.

        Returns:
            dict | list: Clean, serializable representation of the feed.
        """
        pass

    @abstractmethod
    def save_raw(self, feed):
        """
        Save the raw GTFS feed (converted to a dict) to disk (data/raw).

        Args:
            feed (gtfs_realtime_pb2.FeedMessage): Parsed protobuf feed.
        """
        pass

    @abstractmethod
    def save_clean(self, feed):
        """
        Save the cleaned GTFS feed to disk (data/clean).

        Args:
            feed (gtfs_realtime_pb2.FeedMessage): Parsed protobuf feed.
        """
        pass

    def fetch_raw(self):
        """
        Fetch raw GTFS-Realtime bytes from the configured endpoint.

        Returns:
            bytes: Raw protobuf response content.

        Raises:
            requests.exceptions.Timeout: Request exceeded timeout.
            requests.exceptions.HTTPError: Non-200 response.
            requests.exceptions.ConnectionError: Network issue.
            Exception: Any unexpected error.
        """
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
        """
        Parse raw GTFS-Realtime binary data into a FeedMessage object.

        Args:
            raw_bytes (bytes): Binary protobuf message.

        Returns:
            gtfs_realtime_pb2.FeedMessage: Parsed feed.
        """
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw_bytes)
        return feed

    def to_raw_dict(self, feed):
        """
        Convert a FeedMessage object into a raw dictionary using protobuf's JSON formatter.

        Args:
            feed (gtfs_realtime_pb2.FeedMessage): Parsed feed.

        Returns:
            dict: JSON-serializable dictionary matching the GTFS-Realtime format.
        """
        feed_dict = MessageToDict(feed)
        return feed_dict

    def run(self):
        """
        Execute the full fetch-parse-save pipeline for this realtime feed.

        Steps:
            1. fetch raw bytes
            2. parse protobuf feed
            3. save raw JSON
            4. save cleaned JSON
        """ 
        raw = self.fetch_raw()
        feed = self.parse(raw)
        self.save_raw(feed)
        self.save_clean(feed)


class PositionsFetcher(RealtimeFetcher):
    """
    Fetcher for GTFS-Realtime vehicle position updates.

    Fetches vehicle positions, extracts latitude/longitude and trip metadata,
    and stores both raw and cleaned outputs.
    """
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
        """
        Convert a vehicle positions feed into a list of cleaned position records.

        Args:
            feed (gtfs_realtime_pb2.FeedMessage): Parsed positions feed.

        Returns:
            list[dict]: Cleaned vehicle position entries.
        """
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
    """
    Fetcher for GTFS-Realtime trip updates.

    Extracts arrival/departure times, delays, and associated trip metadata.
    """
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
        """
        Convert a trip updates feed into cleaned trip update objects.

        Args:
            feed (gtfs_realtime_pb2.FeedMessage): Parsed trip updates feed.

        Returns:
            list[dict]: List of cleaned trip update entries.
        """
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
    """
    Fetcher for GTFS-Realtime service alerts.

    Extracts alert cause/effect, description, and informed entities.
    """

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
        """
        Convert an alerts feed into cleaned alert objects.

        Args:
            feed (gtfs_realtime_pb2.FeedMessage): Parsed alerts feed.

        Returns:
            list[dict]: List of cleaned alert entries.
        """
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
    """
    Initialize a top-level logger for the realtime data pipeline.

    Args:
        now (str): Timestamp string used to name the log file.

    Returns:
        logging.Logger: Configured logger instance.
    """
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
    """
    Run all realtime fetchers (positions, trip updates, service alerts).

    Args:
        timestamp (str | None): Optional timestamp override. If not provided,
            the current time is used.

    Side Effects:
        Creates log files, fetches remote data, and writes raw/clean JSON output.
    """
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
