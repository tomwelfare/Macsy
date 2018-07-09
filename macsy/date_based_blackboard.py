from macsy.blackboard import Blackboard
from macsy.blackboard_cursor import BlackboardCursor
import pymongo
import sys

__all__ = ['DateBasedBlackboard']

class DateBasedBlackboard(Blackboard):

	def __init__(self, database, blackboard_name, admin_mode=False):
		super().__init__(database, blackboard_name, admin_mode=admin_mode)

		self.__tag_collection = self.__db[blackboard_name + '_TAGS']
		self.__counter_collection = self.__db[blackboard_name + '_COUNTER']
		self.__max_year = 0
		self.__min_year = 99999
		self._populate_document_collections()

	def _populate_document_collections(self):
		colls = ((coll.split('_')[-1], coll) for coll in self.__db.collection_names() if self.__name in coll)

		try:
			colls = {(int(year), self.__db[coll]) for year, coll in colls if year.isdigit()}
		except IndexError:
			print('Blackboard is not date-based. Exiting...')
			sys.exit(0)

		self.__document_collections = colls
		self.__max_year = max(colls.keys())
		self.__min_year = min(colls.keys())

	def count(self):
		return sum(coll.count() for coll in self.__document_collections)

	def find(self, **kwargs):
		max_docs = kwargs.pop('max', 0)
		sort = [(Blackboard.doc_id, kwargs.pop('sort', pymongo.DESCENDING))]
		query = self._build_query(**kwargs)
		results = [self.__document_collections[year].find(query).limit(max_docs).sort(sort) for year in range(self.__min_year, self.__max_year+1)]
		return BlackboardCursor(results)

	def get_earliest_date(self):
		return self.__get_extremal_date(self.__min_year, pymongo.ASCENDING)

	def get_latest_date(self):
		return self.__get_extremal_date(self.__max_year, pymongo.DESCENDING)

	def __get_extremal_date(self, year, order):
		doc = self.__document_collections[year].find().sort({Blackboard.doc_id : order}).limit(1)
		return self.get_date(doc)

	def get_date(self, doc):
		# Not yet implemented
		return None
