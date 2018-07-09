import pymongo

__all__ = ['Blackboard']

class Blackboard():

	#Error codes
	error_doc_not_found = None
	error_doc_exists = -1
	error_tag_not_found = 0
	error_tag_exists = -1

	# Predefined tags and fields
	doc_id = '_id'
	doc_tags = 'Tg'
	doc_for_tags = 'FOR'

	tag_id = '_id'
	tag_name = 'Nm'
	tag_control = 'Ctrl'
	tag_inherit = 'DInh'
	tag_control_for = 'FOR>'
	tag_control_post = 'POST>'

	counter_next = 'NEXT_ID'
	counter_type = 'BLACKBOARD_TYPE'
	counter_type_standard = 'STANDARD'
	counter_type_date_based = 'DATE_BASED'

	def __init__(self, database, blackboard_name, admin_mode=False):
		self._db = database
		self._name = blackboard_name
		self._admin_mode = admin_mode
		self._document_collection = self._db[blackboard_name]
		self._tag_collection = self._db[blackboard_name + '_TAGS']
		self._counter_collection = self._db[blackboard_name + '_COUNTER']

	def count(self):
		return self._document_collection.count()

	def find(self, **kwargs):
		max_docs = kwargs.pop('max', 0)
		sort = [(Blackboard.doc_id, kwargs.pop('sort', pymongo.DESCENDING))]
		query = self._build_query(**kwargs)
		return self._document_collection.find(query).limit(max_docs).sort(sort)

	def _build_query(self, **kwargs):
		#{ "_id" : { "$gte" : { "$oid" : "5b3e91366484863ad4e06a9b"} , "$lt" : { "$oid" : "5b3e91366484863ad4e06a9c"}} , "with" : { "$exists" : true} , "without" : { "$exists" : false} , "Tg" : { "$all" : [ 1]}}
		query = {}
		if 'tags' in kwargs:
			query['Tg'] = {"$all" : [int(x) for x in kwargs.pop('tags', [])]}
		return query

	def get_tag(self, tag_id=None, tag_name = None):
		if tag_id is not None:
			return self._tag_collection.find_one({Blackboard.tag_id : tag_id})
		if tag_name is not None:
			return self._tag_collection.find_one({Blackboard.tag_name : tag_name})
