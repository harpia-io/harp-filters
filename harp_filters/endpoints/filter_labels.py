from microservice_template_core.tools.flask_restplus import api
from flask_restx import Resource
import traceback
from microservice_template_core.tools.logger import get_logger
from harp_filters.models.filter_labels import FilterLabels, FilterLabelsSchema
from flask import request
from werkzeug.exceptions import BadRequest
from microservice_template_core.decorators.auth_decorator import token_required

logger = get_logger()
ns = api.namespace('api/v1/filters/labels', description='Harp endpoint with filter labels')
filter_labels = FilterLabelsSchema()


@ns.route('')
class CreateLabel(Resource):
    @staticmethod
    @api.response(200, 'Label has been created')
    @api.response(400, 'Label already exist')
    @api.response(500, 'Unexpected error on backend side')
    @token_required()
    def put():
        """
        Create New Labels directly via API
        Use this method to create New Label directly via API
        * Send a JSON object
        ```
            {
                "label_name": "some_prometheus_key",
                "label_values": ["value_1", "value_2", "value_3"]
            }
        ```
        """
        try:
            data = filter_labels.load(request.get_json())
            new_obj = FilterLabels.create_label(data)
            result = filter_labels.dump(new_obj.dict())
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


@ns.route('/<label_id>')
class UpdateLabel(Resource):
    @staticmethod
    @api.response(200, 'Label has been update')
    @api.response(400, 'Label already exist')
    @api.response(500, 'Unexpected error on backend side')
    @token_required()
    def post(label_id):
        """
        Update existing Label
        Use this method to update existing Label directly via API
        * Send a JSON object
        ```
            {
                "label_name": "some_prometheus_key",
                "label_values": ["value_1", "value_2", "value_3"]
            }
        ```
        """
        if not label_id:
            return {'msg': 'label_id should be specified'}, 404
        obj = FilterLabels.obj_exist(label_id)
        if not obj:
            return {'msg': f'Label with specified id - {label_id} is not exist'}, 404
        try:
            data = filter_labels.load(request.get_json())
            obj.update_obj(data, label_id=label_id)
            result = filter_labels.dump(obj.dict())
        except ValueError as val_exc:
            logger.warning(
                msg=f"Label updating exception \nException: {str(val_exc)} \nTraceback: {traceback.format_exc()}",
                extra={'tags': {}})
            return {"msg": str(val_exc)}, 400
        except BadRequest as bad_request:
            logger.warning(
                msg=f"Label updating exception \nException: {str(bad_request)} \nTraceback: {traceback.format_exc()}",
                extra={'tags': {}})
            return {'msg': str(bad_request)}, 400
        except Exception as exc:
            logger.critical(
                msg=f"Label updating exception \nException: {str(exc)} \nTraceback: {traceback.format_exc()}",
                extra={'tags': {}})
            return {'msg': 'Exception raised. Check logs for additional info'}, 500
        return result, 200

    @staticmethod
    @token_required()
    def get(label_id):
        """
            Return Label object with specified id
        """
        if not label_id:
            return {'msg': 'label_id should be specified'}, 404
        obj = FilterLabels.obj_exist(label_id)
        if not obj:
            return {'msg': f'object with label_id - {label_id} is not found'}, 404
        result = filter_labels.dump(obj.dict())
        return result, 200

    @staticmethod
    @token_required()
    def delete(label_id):
        """
            Delete Label object with specified id
        """
        if not label_id:
            return {'msg': 'label_id should be specified'}, 404
        obj = FilterLabels.obj_exist(label_id)
        try:
            if obj:
                obj.delete_obj()
                logger.info(
                    msg=f"Label deletion. Id: {label_id}",
                    extra={})
            else:
                return {'msg': f'Object with specified label_id - {label_id} is not found'}, 404
        except Exception as exc:
            logger.critical(
                msg=f"Label deletion exception \nException: {str(exc)} \nTraceback: {traceback.format_exc()}",
                extra={'tags': {}})
            return {'msg': f'Deletion of label with id: {label_id} failed. '
                           f'Exception: {str(exc)}'}, 500
        return {'msg': f"Label with id: {label_id} successfully deleted"}, 200


@ns.route('/all')
class AllLabels(Resource):
    @staticmethod
    @api.response(200, 'Info has been collected')
    @token_required()
    def get():
        """
        Return All exist Labels
        """
        new_obj = FilterLabels.get_all_labels()

        result = {'labels': new_obj}

        return result, 200
