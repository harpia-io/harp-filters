from microservice_template_core.tools.prometheus_metrics import Gauge, Counter, Summary, Histogram


class Prom:
    LABELS_AGGREGATOR = Summary('labels_aggregator_latency_seconds', 'Time spent processing labels aggregator')
