from macsy.blackboard import Blackboard
from macsy.blackboard_cursor import BlackboardCursor
import pymongo
import sys

__all__ = ['DateBasedBlackboard']

class DateBasedBlackboard(Blackboard):

	def __init__(self, database, blackboard_name, admin_mode=False):
		blackboard_name = blackboard_name.upper()
		super().__init__(database, blackboard_name, admin_mode=admin_mode)

		self._tag_collection = self._db[blackboard_name + '_TAGS']
		self._counter_collection = self._db[blackboard_name + '_COUNTER']
		self._max_year = 0
		self._min_year = 99999
		self._populate_document_collections()

	def _populate_document_collections(self):
		colls = ((coll.split('_')[-1], coll) for coll in self._db.collection_names() if self._name in coll)

		try:
			colls = {int(year): self._db[coll] for year, coll in colls if year.isdigit()}
		except IndexError:
			print('Blackboard is not date-based. Exiting...')
			sys.exit(0)

		self._document_collections = colls
		self._max_year = max(colls.keys())
		self._min_year = min(colls.keys())

	def count(self):
		return sum(coll.count() for coll in self._document_collections.values())

	def find(self, **kwargs):
		max_docs = kwargs.pop('max', 0)
		sort = [(Blackboard.doc_id, kwargs.pop('sort', pymongo.DESCENDING))]
		query = self._build_query(**kwargs)
		results = [self._document_collections[year].find(query).limit(max_docs).sort(sort) for year in range(self._min_year, self._max_year+1)]
		return BlackboardCursor(results)

	def get_earliest_date(self):
		return self._get_extremal_date(self._min_year, pymongo.ASCENDING)

	def get_latest_date(self):
		return self._get_extremal_date(self._max_year, pymongo.DESCENDING)

	def _get_extremal_date(self, year, order):
		doc = self._document_collections[year].find().sort({Blackboard.doc_id : order}).limit(1)
		return self.get_date(doc)

	def get_date(self, doc):
		return doc[Blackboard.doc_id].generation_time
