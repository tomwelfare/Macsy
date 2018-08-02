from collections import namedtuple
from datetime import datetime
from dateutil import parser as dtparser
from bson.objectid import ObjectId
from macsy.blackboards.managers import tag_manager, document_manager
TagManager = tag_manager.TagManager
DocumentManager = document_manager.DocumentManager

class QueryBuilder():

    def __init__(self, blackboard):
        self._blackboard = blackboard

    def build_document_query(self, **kwargs):
        Executor = namedtuple('Executor', ['operation','function'])
        executors = {'tags' : Executor('$all', self._build_tag_query), 
            'without_tags' : Executor('$nin', self._build_tag_query), 
            'fields' : Executor(True, self._build_field_query), 
            'without_fields' : Executor(False, self._build_field_query), 
            'min_date' : Executor('$gte', self._build_date_query), 
            'max_date' : Executor('$lt', self._build_date_query)}
        existing = {key : value for key, value in kwargs.items() if key in set(kwargs).intersection(executors) and self._argument_is_list(value)}
        query = {}
        for keyword, value_list in existing.items():
            for value in value_list:
                key, val = executors[keyword].function((query, value, executors[keyword].operation))
                query[key] = val
        return query

    def build_document_update(self, doc_id, updated_fields):
        add_to_set = self._append_list_fields(updated_fields)
        if self._blackboard.document_manager.doc_id in updated_fields: del updated_fields[self._blackboard.document_manager.doc_id]
        update = {"$set" : updated_fields, "$addToSet" : add_to_set} if len(add_to_set) else {"$set" : updated_fields}
        return update

    def build_tags_update_query(self, tag_ids, operation):
        ctrl_tags = [tag_id for tag_id in tag_ids if self._blackboard.tag_manager.is_control_tag(tag_id)]
        normal_tags = [x for x in tag_ids if x not in ctrl_tags]
        query = {operation : {}}
        for tags, field in [(ctrl_tags, self._blackboard.document_manager.doc_control_tags), (normal_tags, self._blackboard.document_manager.doc_tags)]:
            query[operation][field] = {"$each" : tags} if operation == "$addToSet" else tags
        return query

    def build_tag_update_query(self, tag_id, operation):
        field = self._blackboard.document_manager.doc_control_tags if self._blackboard.tag_manager.is_control_tag(tag_id) else self._blackboard.document_manager.doc_tags
        query = {operation : {field:  tag_id}} 
        return query

    def _build_date_query(self, qdv):
        query, date, value = qdv
        q = query.get(self._blackboard.document_manager.doc_id, {})
        q[value] = ObjectId.from_datetime(dtparser.parse(str(date)))
        return (self._blackboard.document_manager.doc_id, q)

    def _build_tag_query(self, qtv):
        query, tag, value = qtv
        tag_m = self._blackboard.tag_manager
        doc_m = self._blackboard.document_manager
        full_tag = tag_m.get_canonical_tag(tag)

        field = doc_m.doc_control_tags if (tag_m.tag_control in full_tag and full_tag[tag_m.tag_control]) else doc_m.doc_tags
        if field in query and "$exists" in query[field]: del query[field]

        q = query.get(field, {value : [int(full_tag[tag_m.tag_id])]})
        if int(full_tag[tag_m.tag_id]) not in q[value]:
            q[value].append(int(full_tag[tag_m.tag_id]))

        return (field, q)

    def _build_field_query(self, qfv):
        query, field, value = qfv
        return (field, query.get(field, {"$exists" : value}))

    def _append_list_fields(self, updated_fields):
        keys = [key for key, value in updated_fields.items() if (key in self._blackboard.document_manager.array_fields or isinstance(value, list))]
        return {key : {'$each' : self._listify(updated_fields.pop(key))} for key in keys}

    def _listify(self, obj):
        return obj if isinstance(obj,list) else [obj]

    def _argument_is_list(self, argument):
        if not isinstance(argument,list):
            raise ValueError('Argument needs to be a list: {}'.format(argument))
        return True