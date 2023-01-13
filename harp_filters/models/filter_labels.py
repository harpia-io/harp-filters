import traceback
from microservice_template_core import db
import datetime
from microservice_template_core.tools.logger import get_logger
from marshmallow import Schema, fields
import ujson as json
from harp_filters.metrics.service_monitoring import Prom

logger = get_logger()


class FilterLabels(db.Model):
    __tablename__ = 'filter_labels'

    label_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    label_name = db.Column(db.VARCHAR(254), nullable=False, unique=True)
    label_values = db.Column(db.Text(4294000000), nullable=False, unique=False)
    create_ts = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow(), nullable=False)
    last_update_ts = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow(), nullable=False)

    def __repr__(self):
        return f"{self.label_id}_{self.label_name}"

    def dict(self):
        return {
            'label_id': self.label_id,
            'label_name': self.label_name,
            'label_values': json.loads(self.label_values),
            'create_ts': self.create_ts,
            'last_update_ts': self.last_update_ts
        }

    @classmethod
    def create_label(cls, data):
        exist_name = cls.query.filter_by(label_name=data['label_name']).one_or_none()
        if exist_name:
            raise ValueError(f"Label Name: {data['label_name']} already exist")
        new_obj = FilterLabels(
            label_name=data['label_name'],
            label_values=json.dumps(data['label_values'])
        )
        new_obj = new_obj.save()
        return new_obj

    @classmethod
    @Prom.LABELS_AGGREGATOR.time()
    def aggr_label(cls, data):
        for label_name, label_value in data.items():
            exist_name = cls.query.filter_by(label_name=label_name).one_or_none()
            if exist_name:
                exist_value = json.loads(exist_name.label_values)
                if label_value not in exist_value:
                    logger.info(msg=f"Update value for existing label - {label_name}. Value - {label_value}")
                    exist_value.append(label_value)
                    cls.query.filter_by(label_name=label_name).update({'label_values': json.dumps(exist_value)})
                    db.session.commit()
            else:
                logger.info(msg=f"Add new label - {label_name} with value - {label_value}")
                new_obj = FilterLabels(
                    label_name=label_name,
                    label_values=json.dumps([label_value])
                )
                new_obj.save()

    @classmethod
    def obj_exist(cls, label_id):
        return cls.query.filter_by(label_id=label_id).one_or_none()

    def update_obj(self, data, label_id):
        data['label_values'] = json.dumps(data['label_values'])

        self.query.filter_by(label_id=label_id).update(data)

        db.session.commit()

    def labels_info_dict(self):
        return {
            'label_id': self.label_id,
            'label_name': self.label_name,
            'label_values': json.loads(self.label_values)
        }

    @classmethod
    def get_all_labels(cls):
        get_all_labels = cls.query.filter_by().all()
        all_labels = [single_event.labels_info_dict() for single_event in get_all_labels]

        return all_labels

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


class FilterLabelsSchema(Schema):
    label_id = fields.Int(dump_only=True)
    label_name = fields.Str(required=True)
    label_values = fields.List(cls_or_instance=fields.Str, required=True)
    create_ts = fields.DateTime("%Y-%m-%d %H:%M:%S", dump_only=True)
    last_update_ts = fields.DateTime("%Y-%m-%d %H:%M:%S", dump_only=True)