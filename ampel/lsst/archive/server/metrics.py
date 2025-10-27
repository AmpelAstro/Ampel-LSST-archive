from prometheus_client import Histogram

REQ_TIME = Histogram("req_time_seconds", "time spent in requests", ("method",))
