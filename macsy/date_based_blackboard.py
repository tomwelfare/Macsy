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

	def count(self, **kwargs):
		query = kwargs.get('query', self._build_query(**kwargs))
		return sum(coll.find(query).count() for coll in self._document_collections.values())

	def get_earliest_date(self):
		return self._get_extremal_date(self._min_year, pymongo.ASCENDING)

	def get_latest_date(self):
		return self._get_extremal_date(self._max_year, pymongo.DESCENDING)

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

	def _get_result(self, qms):
		return [self._document_collections[year].find(qms[0]).limit(qms[1]).sort(qms[2]) for year in range(self._min_year, self._max_year+1)]

	def _get_extremal_date(self, year, order):
		doc = self._document_collections[year].find().sort({Blackboard.doc_id : order}).limit(1)
		return self.get_date(doc)

