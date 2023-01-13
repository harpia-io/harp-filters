import os

NOTIFICATIONS_DECORATED_TOPIC = os.getenv('NOTIFICATIONS_DECORATED_TOPIC', 'dev_collector-notifications-decorated')
KAFKA_SERVERS = os.getenv('KAFKA_SERVERS', '127.0.0.1:9092')
