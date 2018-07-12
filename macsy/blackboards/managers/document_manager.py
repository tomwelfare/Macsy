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
        
    def find(self, **kwargs):
        settings = (kwargs.get('query', self._build_query(**kwargs)), 
            kwargs.pop('max', 0), 
            [(DocumentManager.doc_id, kwargs.pop('sort', pymongo.DESCENDING))])
        return self._get_result(settings)

    def count(self, **kwargs):
        query = kwargs.get('query', self._build_query(**kwargs))
        return self._collection.find(query).count()

    def insert(self, doc):
        if DocumentManager.doc_id not in doc:
            doc[DocumentManager.doc_id] = ObjectId.from_datetime(datetime.now())
        if self._doc_exists(doc):
            doc_id = doc[DocumentManager.doc_id]
            del doc[DocumentManager.doc_id]
            self.update(doc_id, doc)
        return self._collection.insert(doc)

    def update(self, doc_id, updated_fields):
        keys = [key for key, value in updated_fields.items() if type(value) is list]
        add_to_set = {key : {'$each': updated_fields.pop(key)} for key in keys}
        if len(add_to_set):
            return self._collection.update({DocumentManager.doc_id : doc_id}, {"$set" : updated_fields, "$push" : add_to_set})    
        return self._collection.update({DocumentManager.doc_id : doc_id}, {"$set" : updated_fields})

    def delete(self, doc_id):
        return self._collection.remove({DocumentManager.doc_id : doc_id})

    def add_tag(self, doc_id, tag_id):
        return self._add_remove_tag((doc_id, tag_id), '$push')
    
    def remove_tag(self, doc_id, tag_id):
        return self._add_remove_tag((doc_id, tag_id), '$pull')

    # Should check for hash values, not just on id?
    def _doc_exists(self, doc):
        return bool(self.count(query={DocumentManager.doc_id : doc[DocumentManager.doc_id]}))

    def _add_remove_tag(self, ids, operation):
        doc_id, tag_id = ids
        field = DocumentManager.doc_control_tags if self._parent._tag_manager.is_control_tag(tag_id) else DocumentManager.doc_tags
        return self.update({DocumentManager.doc_id : doc_id}, {operation : {field:  tag_id}})

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
        q = query.get(DocumentManager.doc_id, {})
        q[value] = ObjectId.from_datetime(dtparser.parse(str(date)))
        return DocumentManager.doc_id, q

    def _build_tag_query(self, qtv):
        query, tag, value = qtv
        full_tag = self._parent._tag_manager.get_canonical_tag(tag)
        field = DocumentManager.doc_control_tags if (TagManager.tag_control in full_tag \
            and full_tag[TagManager.tag_control]) else DocumentManager.doc_tags
        if field in query and '$exists' in query[field]: del query[field]
        q = query.get(field, {value : [int(full_tag[TagManager.tag_id])]})
        if int(full_tag[TagManager.tag_id]) not in q[value]:
            q[value].append(int(full_tag[TagManager.tag_id]))
        return field, q

    def _build_field_query(self, qfv):
        query, field, value = qfv
        return field, query.get(field, {'$exists' : value})