import pymongo
from bson import ObjectId
from macsy.managers import document_manager

class DateBasedDocumentManager(document_manager.DocumentManager):

    def __init__(self, parent):
        super().__init__(parent)
        self._populate_document_collections()

    def _populate_document_collections(self):
        colls = ((coll.split('_')[-1], coll) for coll in self._parent._db.collection_names() if self._parent._name in coll)
        try:
            colls = {int(year): self._parent._db[coll] for year, coll in colls if year.isdigit()}
        except IndexError:
            raise ValueError('Blackboard is not date-based.')

        self._document_collections = colls
        self._max_year = max(colls.keys())
        self._min_year = min(colls.keys())

    def count(self, **kwargs):
        query = kwargs.get('query', self._build_query(**kwargs))
        return sum(coll.find(query).count() for coll in self._document_collections.values())

    def insert(self, doc):
        raise NotImplementedError()

    def update(self, doc_id, doc):
        raise NotImplementedError()

    def delete(self, doc_id):
        year = self._get_doc_year({DateBasedDocumentManager.doc_id : doc_id})
        return self._document_collections[year].remove({DateBasedDocumentManager.doc_id : doc_id})

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
        return [self._document_collections[year].find(query).limit(max_docs).sort(sort) for year in range(self._min_year, self._max_year+1)]

    def _get_extremal_date(self, year, order):
        return self.get_date(self._document_collections[year].find().sort(DateBasedDocumentManager.doc_id, order).limit(1)[0])

    def _get_doc_year(self, doc):
        return self.get_date(doc).year

    def _add_remove_tag(self, ids, operation):
        doc_id, tag_id = ids
        doc = {DateBasedDocumentManager.doc_id : doc_id}
        year = self._get_doc_year(doc)
        field = DateBasedDocumentManager.doc_control_tags if self._parent._tag_manager.is_control_tag(tag_id) else DateBasedDocumentManager.doc_tags
        return self._document_collections[year].update(doc, {operation : {field:  tag_id}})