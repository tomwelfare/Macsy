from macsy.blackboard import Blackboard
from macsy.blackboard_cursor import BlackboardCursor
import pymongo
import sys

__all__ = ['DateBasedBlackboard']

class DateBasedBlackboard(Blackboard):

	def __init__(self, database, blackboard_name, admin_mode=False):
		self.__db = database
		self.__name = blackboard_name
		self.__admin_mode = admin_mode
		self.__tag_collection = self.__db[blackboard_name + '_TAGS']
		self.__counter_collection = self.__db[blackboard_name + '_COUNTER']
		self.__max_year = 0
		self.__min_year = 99999
		self._populate_document_collections(blackboard_name)
		
	def _populate_document_collections(self, blackboard_name):
		self.__document_collections = {}
		for coll in self.__db.collection_names():
			try:
				if blackboard_name in coll and coll.split('_')[1].isdigit():
					year = int(coll.split('_')[1])
					self.__document_collections[year] = self.__db[coll]
					self.__max_year = max(self.__max_year, year)
					self.__min_year = min(self.__min_year, year)
			except IndexError:
				print('Blackboard is not date-based. Exiting...')
				sys.exit(0)

	def count(self):
		return sum(self.__document_collections[year].count() for year in range(self.__min_year, self.__max_year+1))

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
