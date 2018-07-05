from macsy.blackboard import Blackboard
from macsy.blackboard_cursor import BlackboardCursor
import pymongo

__all__ = ['DateBasedBlackboard']

class DateBasedBlackboard(Blackboard):

	# db objects and settings
	__db = None
	__name = None
	__admin_mode = False

	__document_collections = {}
	__tag_collection = None
	__counter_collection = None

	__max_year = 0
	__min_year = 99999

	def __init__(self, database, blackboard_name, admin_mode=False):
		self.__db = database
		self.__name = blackboard_name
		self.__admin_mode = admin_mode
		self.__tag_collection = self.__db[blackboard_name + '_TAGS']
		self.__counter_collection = self.__db[blackboard_name + '_COUNTER']
		self._populate_document_collections(blackboard_name)
		

	def _populate_document_collections(self, blackboard_name):
		for coll in self.__db.collection_names():
			if blackboard_name in coll and coll.split('_')[1].isdigit():
				year = coll.split('_')[1]
				self.__document_collections[int(year)] = self.__db[coll]
				self.__max_year = max(self.__max_year, year)
				self.__min_year = min(self.__min_year, year)
	
	def count(self):
		total = 0
		for year in range(self.__min_year, self.__max_year):
			total += self.__document_collections[year].count()
		return total

	def find(self, **kwargs):
		max_docs = kwargs.pop('max', 0)
		sort = { Blackboard.doc_id : kwargs.pop('sort', pymongo.DESCENDING)}
		query = self._build_query(kwargs)
		results = []
		for year in range(self.__min_year, self.__max_year): # assumes no date given
			results.append(self.__document_collections[year].find(query).limit(max_docs).sort(sort))
		return BlackboardCursor(results)

	def get_earliest_date(self):
		doc = self.__document_collections[self.__min_year].find().sort({Blackboard.doc_id : pymongo.ASCENDING}).limit(1)
		return self.get_date(doc)

	def get_latest_date(self):
		doc = self.__document_collections[self.__max_year].find().sort({Blackboard.doc_id : pymongo.DESCENDING}).limit(1)
		return self.get_date(doc)

	def get_date(self, doc):
		raise new NotImplementedError()






