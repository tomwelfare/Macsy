import pymongo
#from bson.objectid import ObjectId
from macsy.blackboards.managers import base_manager, tag_manager
TagManager = tag_manager.TagManager


class DocumentManager(base_manager.BaseManager):

    doc_id = '_id'
    doc_tags = 'Tg'
    doc_control_tags = 'FOR'

    def __init__(self, blackboard):
        super().__init__(blackboard, '')
        self.array_fields = [DocumentManager.doc_tags, DocumentManager.doc_control_tags]
        self.doc_id = DocumentManager.doc_id
        self.doc_tags = DocumentManager.doc_tags
        self.doc_control_tags = DocumentManager.doc_control_tags
        
    def find(self, **kwargs):
        query = kwargs.get('query', self._query_builder.build_document_query(**kwargs))
        sort = [(self.doc_id, kwargs.pop('sort', pymongo.DESCENDING))]
        max_docs = kwargs.pop('max', 0)
        return (self._collection.find(query).sort(sort).limit(max_docs), max_docs)

    def count(self, **kwargs):
        query = kwargs.get('query', self._query_builder.build_document_query(**kwargs))
        return self._collection.find(query).count()

    def insert(self, doc):
        doc[self.doc_id] = self._get_or_generate_id(doc)
        self._ensure_array_fields(doc)
        return self.update(doc[self.doc_id], doc) if self._doc_exists(doc) else self._collection.insert(doc)

    def update(self, doc_id, updated_fields):
        update = self._query_builder.build_document_update(doc_id, updated_fields)
        response = self._collection.update({self.doc_id : doc_id}, update)
        return doc_id if response['updatedExisting'] else None

    def delete(self, doc_id):
        return self._collection.remove({self.doc_id : doc_id})

    def update_document_tags(self, ids, operations):
        doc_id, tag_id = ids
        update = self._query_builder.build_tags_update_query(tag_id, operations[0]) if isinstance(tag_id, list) else \
            self._query_builder.build_tag_update_query(tag_id, operations[1])
        return self._collection.update({self.doc_id : doc_id}, update)

    # Should check for hash values, not just on id?
    def _doc_exists(self, doc):
        return bool(self.count(query={self.doc_id : doc[self.doc_id]}))

    def _get_or_generate_id(self, doc):
        if self.doc_id not in doc:
            return self._blackboard.counter_manager.get_next_id_and_increment(self._blackboard.counter_manager.counter_doc)
        return doc[self.doc_id]

    def _ensure_array_fields(self, doc):
        missing_tags = {field : [] for field in self.array_fields if field not in doc}
        doc.update(missing_tags)
