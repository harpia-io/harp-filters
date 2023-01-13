from microservice_template_core.settings import ServiceConfig
from microservice_template_core.tools.kafka_confluent_consumer import KafkaConsumeMessages
import harp_filters.settings as settings
from microservice_template_core.tools.logger import get_logger
import ujson as json
from harp_filters.models.filter_labels import FilterLabels

logger = get_logger()


class ConsumeMessages(object):
    def __init__(self):
        pass

    def start_consumer(self, consumer_num=ServiceConfig.SERVICE_NAME):
        """
        Start metrics consumer
        """
        consumer = KafkaConsumeMessages(kafka_topic=settings.NOTIFICATIONS_DECORATED_TOPIC).start_consumer()

        while True:
            msg = consumer.poll(5.0)

            if msg is None:
                continue
            if msg.error():
                logger.error(msg=f"Consumer error: {msg.error()}")
                continue

            parsed_json = json.loads(msg.value().decode('utf-8'))

            logger.info(
                msg=f"Get event from Kafka:\nJSON: {parsed_json}.\nConsumer: {consumer_num}",
                extra={'tags': {
                    'event_id': parsed_json['event_id']
                }}
            )

            main_fields = {
                'alert_name': parsed_json['alert_name'],
                'source': parsed_json['source'],
                'monitoring_system': parsed_json['monitoring_system'],
            }

            data = {**main_fields, **parsed_json['additional_fields']}

            FilterLabels.aggr_label(data=data)

    def main(self):
        self.start_consumer()
