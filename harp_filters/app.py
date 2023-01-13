from microservice_template_core import Core
from microservice_template_core.settings import ServiceConfig, FlaskConfig, DbConfig
from harp_filters.endpoints.filter_config import ns as filter_config
from harp_filters.endpoints.filter_labels import ns as filter_labels
from harp_filters.endpoints.health import ns as health
from harp_filters.logic.labels_processor import ConsumeMessages
import threading
from microservice_template_core.tools.logger import get_logger

logger = get_logger()


def init_consumer():
    logger.debug(msg=f"Initialize consumer")
    worker = ConsumeMessages()
    worker.main()


def start_flask():
    ServiceConfig.configuration['namespaces'] = [filter_config, filter_labels, health]
    FlaskConfig.FLASK_DEBUG = False
    DbConfig.USE_DB = True
    app = Core()
    app.run()


def main():
    logger.debug(msg=f"Start Flask")
    api_service = threading.Thread(name='Web App', target=start_flask, daemon=True)
    api_service.start()
    logger.debug(msg=f"Start Consumer")
    init_consumer()


if __name__ == '__main__':
    main()

