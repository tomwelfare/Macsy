import pymongo
import sys
from bson.objectid import ObjectId
from macsy.blackboards import blackboard
from macsy.blackboards.blackboard import check_admin
Blackboard = blackboard.Blackboard

class DateBasedBlackboard(Blackboard):

    def __init__(self, settings):
        super().__init__((settings[0], settings[1].upper(), settings[2]))
        self._populate_document_collections()

    def count(self, **kwargs):
        query = kwargs.get('query', self._build_query(**kwargs))
        return sum(coll.find(query).count() for coll in self._document_collections.values())

    def insert(self, doc):
        raise NotImplementedError()

    def update(self, doc_id, doc):
        raise NotImplementedError()

    @check_admin('Admin rights required to delete documents.')
    def delete(self, doc_id):
        year = self._get_doc_year({Blackboard.doc_id : doc_id})
        return self._document_collections[year].remove({Blackboard.doc_id : doc_id})

    def get_date(self, doc):
        if Blackboard.doc_id in doc and type(doc[Blackboard.doc_id]) is ObjectId:
            return doc[Blackboard.doc_id].generation_time           
        raise ValueError('Document does not have an ObjectId in the {} field'.format(Blackboard.doc_id))

    def get_earliest_date(self):
        return self._get_extremal_date(self._min_year, pymongo.ASCENDING)

    def get_latest_date(self):
        return self._get_extremal_date(self._max_year, pymongo.DESCENDING)

    def _populate_document_collections(self):
        colls = ((coll.split('_')[-1], coll) for coll in self._db.collection_names() if self._name in coll)
        try:
            colls = {int(year): self._db[coll] for year, coll in colls if year.isdigit()}
        except IndexError:
            raise ValueError('Blackboard is not date-based.')

        self._document_collections = colls
        self._max_year = max(colls.keys())
        self._min_year = min(colls.keys())

    def _get_result(self, qms):
        query, max_docs, sort = qms
        return [self._document_collections[year].find(query).limit(max_docs).sort(sort) for year in range(self._min_year, self._max_year+1)]

    def _get_extremal_date(self, year, order):
        return self.get_date(self._document_collections[year].find().sort(Blackboard.doc_id, order).limit(1)[0])

    def _get_doc_year(self, doc):
        return self.get_date(doc).year

    def _add_remove_tag(self, ids, operation):
        doc_id, tag_id = ids
        doc = {Blackboard.doc_id : doc_id}
        year = self._get_doc_year(doc)
        field = Blackboard.doc_control_tags if self.is_control_tag(tag_id) else Blackboard.doc_tags
        return self._document_collections[year].update(doc, {operation, {field:  tag_id}})

