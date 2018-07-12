import pymongo
from datetime import datetime
from bson import ObjectId
from macsy.blackboards.managers import document_manager

class DateBasedDocumentManager(document_manager.DocumentManager):

    def __init__(self, parent):
        super().__init__(parent)
        self._populate_collections()

    def _populate_collections(self):
        colls = ((coll.split('_')[-1], coll) for coll in self._parent._db.collection_names() if self._parent._name in coll)
        try:
            self._collections = {int(year): self._parent._db[coll] for year, coll in colls if year.isdigit()}
            self._max_year = max(self._collections.keys())
            self._min_year = min(self._collections.keys())    
        except IndexError:
            raise ValueError('Blackboard is not date-based.')

    def count(self, **kwargs):
        query = kwargs.get('query', self._build_query(**kwargs))
        return sum(coll.find(query).count() for coll in self._collections.values())

    def insert(self, doc):
        if DateBasedDocumentManager.doc_id not in doc:
            doc[DateBasedDocumentManager.doc_id] = ObjectId.from_datetime(datetime.now())
        year = self._get_doc_year(doc)
        if self._doc_exists(doc):
            doc_id = doc[DateBasedDocumentManager.doc_id]
            del doc[DateBasedDocumentManager.doc_id]
            self.update(doc_id, doc)
        return self._collections[year].insert(doc)

    def update(self, doc_id, updated_fields):
        year = self._get_doc_year({DateBasedDocumentManager.doc_id : doc_id})
        keys = [key for key, value in updated_fields.items() if type(value) is list]
        add_to_set = {key : {'$each': updated_fields.pop(key)} for key in keys}
        if len(add_to_set):
            return self._collections[year].update({DateBasedDocumentManager.doc_id : doc_id}, {"$set" : updated_fields, "$push" : add_to_set})    
        return self._collections[year].update({DateBasedDocumentManager.doc_id : doc_id}, {"$set" : updated_fields})

    def delete(self, doc_id):
        year = self._get_doc_year({DateBasedDocumentManager.doc_id : doc_id})
        return self._collections[year].remove({DateBasedDocumentManager.doc_id : doc_id})

    def get_date(self, doc):
        if DateBasedDocumentManager.doc_id in doc and type(doc[DateBasedDocumentManager.doc_id]) is ObjectId:
            return doc[DateBasedDocumentManager.doc_id].generation_time           
        raise ValueError('Document does not have an ObjectId in the {} field'.format(DateBasedDocumentManager.doc_id))

    def get_earliest_date(self):
        return self._get_extremal_date(self._min_year, pymongo.ASCENDING)

    def get_latest_date(self):
        return self._get_extremal_date(self._max_year, pymongo.DESCENDING)

    def _get_result(self, qms):
        query, max_docs, sort = qms
        return [self._collections[year].find(query).limit(max_docs).sort(sort) for year in range(self._min_year, self._max_year+1)]

    def _get_extremal_date(self, year, order):
        return self.get_date(self._collections[year].find().sort(DateBasedDocumentManager.doc_id, order).limit(1)[0])

    def _get_doc_year(self, doc):
        return self.get_date(doc).year

    def _add_remove_tag(self, ids, operation):
        doc_id, tag_id = ids
        doc = {DateBasedDocumentManager.doc_id : doc_id}
        year = self._get_doc_year(doc)
        field = DateBasedDocumentManager.doc_control_tags if self._parent._tag_manager.is_control_tag(tag_id) else DateBasedDocumentManager.doc_tags
        return self._collections[year].update(doc, {operation : {field:  tag_id}})