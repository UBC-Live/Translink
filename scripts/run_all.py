from fetch_realtime import run as fetch_realtime
from fetch_static import run as fetch_static
from datetime import datetime

now = datetime.now().strftime("%Y-%m-%dT%H-%M")
fetch_static(now)
fetch_realtime(now)
