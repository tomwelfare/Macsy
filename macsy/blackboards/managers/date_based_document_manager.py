import pymongo
from datetime import datetime
from bson import ObjectId
from macsy.blackboards.managers import document_manager

class DateBasedDocumentManager(document_manager.DocumentManager):

    def __init__(self, parent):
        super().__init__(parent)
        self._populate_collections()
        self.array_fields.extend(['Fds','LOC'])
        self.doc_id = DateBasedDocumentManager.doc_id
        self.doc_tags = DateBasedDocumentManager.doc_tags
        self.doc_control_tags = DateBasedDocumentManager.doc_control_tags

    def _populate_collections(self):
        colls = ((coll.split('_')[-1], coll) for coll in self._parent._db.collection_names() if self._parent._name in coll)
        self._collections = {int(year): self._parent._db[coll] for year, coll in colls if year.isdigit()}
        self._max_year = max(self._collections.keys())
        self._min_year = min(self._collections.keys())

    def count(self, **kwargs):
        query = kwargs.get('query', self._build_query(**kwargs))
        return sum(coll.find(query).count() for coll in self._collections.values())

    def insert(self, doc):
        doc[self.doc_id] = self._get_or_generate_id(doc)
        self._ensure_array_fields(doc)
        year = self._get_doc_year(doc)
        return self.update(doc[self.doc_id], doc) if self._doc_exists(doc) else self._collections[year].insert(doc)

    def update(self, doc_id, updated_fields):
        year = self._get_doc_year({self.doc_id : doc_id})
        add_to_set = self._append_list_fields(updated_fields)
        response = self._collections[year].update({self.doc_id : doc_id}, \
            {"$set" : updated_fields, "$addToSet" : add_to_set}) if len(add_to_set) else \
            self._collections[year].update({self.doc_id : doc_id}, {"$set" : updated_fields})
        return doc_id if response['updatedExisting'] else False

    def delete(self, doc_id):
        year = self._get_doc_year({self.doc_id : doc_id})
        return self._collections[year].remove({self.doc_id : doc_id})

    def get_date(self, doc):
        if self.doc_id in doc and type(doc[self.doc_id]) is ObjectId:
            return doc[self.doc_id].generation_time           
        raise ValueError('Document does not have an ObjectId in the {} field'.format(self.doc_id))

    def get_earliest_date(self):
        return self._get_extremal_date(self._min_year, pymongo.ASCENDING)

    def get_latest_date(self):
        return self._get_extremal_date(self._max_year, pymongo.DESCENDING)

    def _get_or_generate_id(self, doc):
        if self.doc_id not in doc:
            return ObjectId.from_datetime(datetime.now())
        return doc[self.doc_id]

    def _get_result(self, qms):
        query, max_docs, sort = qms
        return [self._collections[year].find(query).limit(max_docs).sort(sort) for year in range(self._min_year, self._max_year+1)]

    def _get_extremal_date(self, year, order):
        return self.get_date(self._collections[year].find().sort(self.doc_id, order).limit(1)[0])

    def _get_doc_year(self, doc):
        return self.get_date(doc).year

    def _add_remove_tag(self, ids, operation):
        doc_id, tag_id = ids
        year = self._get_doc_year({self.doc_id : doc_id})
        field = self.doc_control_tags if self._parent.tag_manager.is_control_tag(tag_id) else self.doc_tags
        return  self._collections[year].update({self.doc_id : doc_id}, {operation : {field:  tag_id}})

    def _add_remove_tags(self, ids, operation):
        doc_id, tag_ids = ids
        year = self._get_doc_year({self.doc_id : doc_id})
        query = self._build_tag_update_query(tag_ids, operation)
        return self._collections[year].update({self.doc_id : doc_id}, query)