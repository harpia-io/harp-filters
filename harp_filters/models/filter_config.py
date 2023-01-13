import json
import traceback
from microservice_template_core import db
import datetime
from microservice_template_core.tools.logger import get_logger
from marshmallow import Schema, fields

logger = get_logger()


class FilterConfig(db.Model):
    __tablename__ = 'filter_config'

    filter_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    filter_name = db.Column(db.VARCHAR(254), nullable=False, unique=True)
    filter_config = db.Column(db.Text(4294000000), nullable=False, unique=False)
    columns = db.Column(db.Text(4294000000), nullable=False, unique=False)
    grouping = db.Column(db.Text(4294000000), nullable=False, unique=False)
    filter_status = db.Column(db.VARCHAR(254), nullable=False, unique=False)
    created_by = db.Column(db.VARCHAR(254), nullable=False, unique=False)
    create_ts = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow(), nullable=False)
    last_update_ts = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow(), nullable=False)

    def __repr__(self):
        return f"{self.filter_id}_{self.filter_name}"

    def dict(self):
        return {
            'filter_id': self.filter_id,
            'filter_name': self.filter_name,
            'filter_config': json.loads(self.filter_config),
            'columns': json.loads(self.columns),
            'grouping': json.loads(self.grouping),
            'filter_status': self.filter_status,
            'created_by': self.created_by,
            'create_ts': self.create_ts,
            'last_update_ts': self.last_update_ts
        }

    @classmethod
    def create_filter(cls, data):
        exist_filter_name = cls.query.filter_by(filter_name=data['filter_name']).one_or_none()
        if exist_filter_name:
            raise ValueError(f"Filter Name: {data['filter_name']} already exist")
        new_obj = FilterConfig(
            filter_name=data['filter_name'],
            filter_config=json.dumps(data['filter_config']),
            columns=json.dumps(data['columns']),
            grouping=json.dumps(data['grouping']),
            filter_status=data['filter_status'],
            created_by=data['created_by']
        )
        new_obj = new_obj.save()
        return new_obj

    @classmethod
    def obj_exist(cls, filter_id):
        return cls.query.filter_by(filter_id=filter_id).one_or_none()

    def update_obj(self, data, filter_id):
        if 'filter_config' in data:
            data['filter_config'] = json.dumps(data['filter_config'])

        if 'columns' in data:
            data['columns'] = json.dumps(data['columns'])

        if 'grouping' in data:
            data['grouping'] = json.dumps(data['grouping'])

        self.query.filter_by(filter_id=filter_id).update(data)

        db.session.commit()

    def filters_info_dict(self):
        return {
            'filter_id': self.filter_id,
            'filter_name': self.filter_name,
            'filter_config': json.loads(self.filter_config),
            'columns': json.loads(self.columns),
            'grouping': json.loads(self.grouping),
            'filter_status': self.filter_status,
            'created_by': self.created_by,
        }

    @classmethod
    def get_all_filters(cls, username):
        global_search_select = []
        global_filter_dict = {}

        get_all_filters = cls.query.filter_by().all()

        for single_event in get_all_filters:
            if single_event.filter_status == 'private' and single_event.created_by != username:
                continue
            else:
                global_search_select.append(
                    {'id': str(single_event.filter_id), 'value': single_event.filter_name, 'filter_status': single_event.filter_status}
                )

            global_filter_dict[str(single_event.filter_id)] = json.loads(single_event.filter_config)

        return {'global_search_select': global_search_select, 'global_filter_dict': global_filter_dict}

    def save(self):
        try:
            db.session.add(self)
            db.session.flush()
            db.session.commit()

            return self
        except Exception as exc:
            logger.critical(
                msg=f"Can't commit changes to DB \nException: {str(exc)} \nTraceback: {traceback.format_exc()}",
                extra={'tags': {}}
            )
            db.session.rollback()

    def delete_obj(self):
        db.session.delete(self)
        db.session.commit()


class FilterConfigSchema(Schema):
    filter_id = fields.Int(dump_only=True)
    filter_name = fields.Str(required=False)
    filter_config = fields.List(required=False, cls_or_instance=fields.Dict)
    columns = fields.List(required=False, cls_or_instance=fields.Dict)
    grouping = fields.List(required=False, cls_or_instance=fields.Str)
    filter_status = fields.Str(required=False)
    created_by = fields.Str(required=False)
    create_ts = fields.DateTime("%Y-%m-%d %H:%M:%S", dump_only=True)
    last_update_ts = fields.DateTime("%Y-%m-%d %H:%M:%S", dump_only=True)
