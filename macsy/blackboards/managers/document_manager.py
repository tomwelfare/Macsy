import pymongo
from bson.objectid import ObjectId
from datetime import datetime
from dateutil import parser as dtparser
from macsy.blackboards.managers import base_manager, tag_manager
TagManager = tag_manager.TagManager

class DocumentManager(base_manager.BaseManager):

    doc_id = '_id'
    doc_tags = 'Tg'
    doc_control_tags = 'FOR'

    def __init__(self, parent):
        super().__init__(parent, '')
        self.array_fields = [DocumentManager.doc_tags, DocumentManager.doc_control_tags]
        self.doc_id = DocumentManager.doc_id
        self.doc_tags = DocumentManager.doc_tags
        self.doc_control_tags = DocumentManager.doc_control_tags
        
    def find(self, **kwargs):
        settings = (kwargs.get('query', self._build_query(**kwargs)), 
            kwargs.pop('max', 0), 
            [(self.doc_id, kwargs.pop('sort', pymongo.DESCENDING))])
        return self._get_result(settings), settings[1]

    def count(self, **kwargs):
        query = kwargs.get('query', self._build_query(**kwargs))
        return self._collection.find(query).count()

    def insert(self, doc):
        doc[self.doc_id] = self._get_or_generate_id(doc)
        self._ensure_array_fields(doc)
        return self.update(doc[self.doc_id], doc) if self._doc_exists(doc) else self._collection.insert(doc)

    def update(self, doc_id, updated_fields):
        add_to_set = self._append_list_fields(updated_fields)
        if self.doc_id in updated_fields: del updated_fields[self.doc_id]
        response =  self._collection.update({self.doc_id : doc_id}, \
            {"$set" : updated_fields, "$addToSet" : add_to_set}) if len(add_to_set) else \
            self._collection.update({self.doc_id : doc_id}, {"$set" : updated_fields})
        if response['updatedExisting']:
            return doc_id
        return None

    def delete(self, doc_id):
        return self._collection.remove({self.doc_id : doc_id})

    def update_document_tags(self, ids, operations):
        return self._add_remove_tags(ids, operations[0]) if type(ids[1]) is list else \
            self._add_remove_tag(ids, operations[1])

    # Should check for hash values, not just on id?
    def _doc_exists(self, doc):
        return bool(self.count(query={self.doc_id : doc[self.doc_id]}))

    def _add_remove_tag(self, ids, operation):
        doc_id, tag_id = ids
        field = self.doc_control_tags if self._parent.tag_manager.is_control_tag(tag_id) else self.doc_tags
        return self._collection.update({self.doc_id : doc_id}, {operation : {field:  tag_id}})

    def _add_remove_tags(self, ids, operation):
        doc_id, tag_ids = ids
        query = self._build_tag_update_query(tag_ids, operation)
        return self._collection.update({self.doc_id : doc_id}, query)

    def _build_tag_update_query(self, tag_ids, operation):
        ctrl_tags = [tag_id for tag_id in tag_ids if self._parent.tag_manager.is_control_tag(tag_id)]
        normal_tags = [x for x in tag_ids if x not in ctrl_tags]
        query = {operation : {}}
        for tags, field in [(ctrl_tags, self.doc_control_tags), (normal_tags, self.doc_tags)]:
            query[operation][field] = {"$each" : tags} if operation == "$addToSet" else tags
        return query

    def _get_result(self, qms):
        query, max_docs, sort = qms
        return self._collection.find(query).limit(max_docs).sort(sort)

    def _build_query(self, **kwargs):
        qw = {'tags' : ('$all', self._build_tag_query, {}), 
            'without_tags' : ('$nin', self._build_tag_query, {}), 
            'fields' : (True, self._build_field_query, {}), 
            'without_fields' : (False, self._build_field_query, {}), 
            'min_date' : ('$gte', self._build_date_query, None), 
            'max_date' : ('$lt', self._build_date_query, None)}
        query = {}
        for k in set(kwargs).intersection(qw):
            for d in kwargs.get(k,qw[k][2]):
                assert type(kwargs.get(k,qw[k][2])) is list, \
                'Argument needs to be a list: {}'.format(kwargs.get(k, qw[k][2]))
                key, value = qw[k][1]((query, d, qw[k][0]))
                query[key] = value

        return query

    def _build_date_query(self, qdv):
        query, date, value = qdv
        q = query.get(self.doc_id, {})
        q[value] = ObjectId.from_datetime(dtparser.parse(str(date)))
        return self.doc_id, q

    def _build_tag_query(self, qtv):
        query, tag, value = qtv
        full_tag = self._parent.tag_manager.get_canonical_tag(tag)
        field = self.doc_control_tags if (TagManager.tag_control in full_tag \
            and full_tag[TagManager.tag_control]) else self.doc_tags
        if field in query and "$exists" in query[field]: del query[field]
        q = query.get(field, {value : [int(full_tag[TagManager.tag_id])]})
        if int(full_tag[TagManager.tag_id]) not in q[value]:
            q[value].append(int(full_tag[TagManager.tag_id]))
        return field, q

    def _build_field_query(self, qfv):
        query, field, value = qfv
        return field, query.get(field, {"$exists" : value})

    def _append_list_fields(self, updated_fields):
        keys = [key for key, value in updated_fields.items() if (key in self.array_fields or type(value) is list)]
        return {key : {'$each' : self._listify(updated_fields.pop(key))} for key in keys}

    def _listify(self, obj):
        return obj if type(obj) is list else [obj]

    def _get_or_generate_id(self, doc):
        if self.doc_id not in doc:
            return self._parent.counter_manager.get_next_id_and_increment(self._parent.counter_manager.counter_doc)
        return doc[self.doc_id]

    def _ensure_array_fields(self, doc):
        missing_tags = {field : [] for field in self.array_fields if field not in doc}
        doc.update(missing_tags)