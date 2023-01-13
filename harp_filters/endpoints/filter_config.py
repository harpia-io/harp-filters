from microservice_template_core.tools.flask_restplus import api
from flask_restx import Resource
import traceback
from microservice_template_core.tools.logger import get_logger
from harp_filters.models.filter_config import FilterConfig, FilterConfigSchema
from flask import request
from werkzeug.exceptions import BadRequest
from microservice_template_core.decorators.auth_decorator import token_required
from harp_filters.logic.token import get_user_id_by_token

logger = get_logger()
ns = api.namespace('api/v1/filters/config', description='Harp endpoint with filter configs')
filter_config = FilterConfigSchema()


@ns.route('')
class CreateFilter(Resource):
    @staticmethod
    @api.response(200, 'Filter has been created')
    @api.response(400, 'Filter already exist')
    @api.response(500, 'Unexpected error on backend side')
    @token_required()
    def put():
        """
        Create New Filter directly via API
        * Send a JSON object
        ```
            {
                "filter_name": "My Filter",
                "columns": [
                    {
                        "name": "notificationName",
                        "placeholder": "Notification name...",
                        "id_": "name",
                        "width": 242
                    }
                ],
                "grouping": ["alert_name"],
                "filter_config": [
                    {
                        "tag": "dc_name",
                        "condition": "equal",
                        "value": "FA0"
                    },
                    {
                        "tag": "monitoring-system",
                        "condition": "equal",
                        "value": "LA0"
                    },
                    {
                        "tag": "prometheus-server",
                        "condition": "equal",
                        "value": "LA0"
                    }
                ],
                "filter_status": "private", # private or public
            }
        ```
        """
        try:
            auth_token = request.headers.get('AuthToken')
            username = get_user_id_by_token(auth_token)

            data = filter_config.load(request.get_json())
            data['created_by'] = username

            new_obj = FilterConfig.create_filter(data)
            result = filter_config.dump(new_obj.dict())
        except ValueError as val_exc:
            logger.warning(
                msg=str(val_exc),
                extra={'tags': {}})
            return {"msg": str(val_exc)}, 400
        except Exception as exc:
            logger.critical(
                msg=f"General exception \nException: {str(exc)} \nTraceback: {traceback.format_exc()}",
                extra={'tags': {}})
            return {'msg': 'Exception raised. Check logs for additional info'}, 500

        return result, 200


@ns.route('/<filter_id>')
class UpdateFilter(Resource):
    @staticmethod
    @api.response(200, 'Filter has been update')
    @api.response(400, 'Filter already exist')
    @api.response(500, 'Unexpected error on backend side')
    @token_required()
    def post(filter_id):
        """
        Update existing Filter
        * Send a JSON object
        ```
            {
                "filter_name": "My Filter",
                "filter_config": "label_1 = value_1 AND label_2 != value_2",
                "filter_status": "private", # private or public,
                "columns": [
                    {
                        "name": "notificationName",
                        "placeholder": "Notification name...",
                        "id_": "name",
                        "width": 242
                    }
                ],
                "grouping": ["alert_name"]
            }
        ```
        """
        if not filter_id:
            return {'msg': 'filter_id should be specified'}, 404
        obj = FilterConfig.obj_exist(filter_id)
        if not obj:
            return {'msg': f'Filter with specified id - {filter_id} is not exist'}, 404
        try:
            data = filter_config.load(request.get_json())
            obj.update_obj(data, filter_id=filter_id)
            result = filter_config.dump(obj.dict())
        except ValueError as val_exc:
            logger.warning(
                msg=f"Filter updating exception \nException: {str(val_exc)} \nTraceback: {traceback.format_exc()}",
                extra={'tags': {}})
            return {"msg": str(val_exc)}, 400
        except BadRequest as bad_request:
            logger.warning(
                msg=f"Filter updating exception \nException: {str(bad_request)} \nTraceback: {traceback.format_exc()}",
                extra={'tags': {}})
            return {'msg': str(bad_request)}, 400
        except Exception as exc:
            logger.critical(
                msg=f"Filter updating exception \nException: {str(exc)} \nTraceback: {traceback.format_exc()}",
                extra={'tags': {}})
            return {'msg': 'Exception raised. Check logs for additional info'}, 500
        return result, 200

    @staticmethod
    @token_required()
    def get(filter_id):
        """
            Return Filter object with specified id
        """
        if not filter_id:
            return {'msg': 'filter_id should be specified'}, 404
        obj = FilterConfig.obj_exist(filter_id)
        if not obj:
            return {'msg': f'object with filter_id - {filter_id} is not found'}, 404
        result = filter_config.dump(obj.dict())
        return result, 200

    @staticmethod
    @token_required()
    def delete(filter_id):
        """
            Delete Filter object with specified id
        """
        if not filter_id:
            return {'msg': 'filter_id should be specified'}, 404
        obj = FilterConfig.obj_exist(filter_id)
        try:
            if obj:
                obj.delete_obj()
                logger.info(
                    msg=f"Filter deletion. Id: {filter_id}",
                    extra={})
            else:
                return {'msg': f'Object with specified filter_id - {filter_id} is not found'}, 404
        except Exception as exc:
            logger.critical(
                msg=f"Filter deletion exception \nException: {str(exc)} \nTraceback: {traceback.format_exc()}",
                extra={'tags': {}})
            return {'msg': f'Deletion of Filter with id: {filter_id} failed. '
                           f'Exception: {str(exc)}'}, 500
        return {'msg': f"Filter with id: {filter_id} successfully deleted"}, 200


@ns.route('/all')
class AllFilters(Resource):
    @staticmethod
    @api.response(200, 'Info has been collected')
    @token_required()
    def get():
        """
        Return All exist Filters
        """

        auth_token = request.headers.get('AuthToken')
        username = get_user_id_by_token(auth_token)

        try:
            new_obj = FilterConfig.get_all_filters(username)

            result = {'filters': new_obj}
        except Exception as err:
            result = {'filters': f'Can`t get all filters - {err}'}
            logger.error(msg=f"Can`t get all filters - {err}. Trace: {traceback.format_exc()}")

        return result, 200
