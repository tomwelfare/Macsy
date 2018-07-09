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
			[self.__build_tag_query(query, tag, "$all") for tag in kwargs.pop('tags', {})]
		elif 'without_tags' in kwargs:
			[self.__build_tag_query(query, tag, "$nin") for tag in kwargs.pop('without_tags', {})]
		if 'fields' in kwargs:
			[self.__build_field_query(query, field, True) for field in kwargs.pop('fields', {})]
		if 'without_fields' in kwargs:
			[self.__build_field_query(query, field, False) for field in kwargs.pop('without_fields', {})]

		print(query)
		return query

	def __build_tag_query(self, query, tag, value):
		if type(tag) is str:
			full_tag = self.get_tag(tag_name=tag)
		else:
			full_tag = self.get_tag(tag_id=tag)

		field = 'Tg'
		if 'Ctrl' in full_tag and full_tag['Ctrl']:
			field = 'FOR'			

		q = query.get(field, {value : []})
		q[value].append(int(full_tag['_id']))
		query[field] = q
		return query

	def __build_field_query(self, query, field, value):
		query[field] = {"$exists" : value}
		return query

	def get_tag(self, tag_id = None, tag_name = None):
		if tag_id is not None:
			return self._tag_collection.find_one({Blackboard.tag_id : tag_id})
		if tag_name is not None:
			return self._tag_collection.find_one({Blackboard.tag_name : tag_name})

	def is_control_tag(self, tag_id = None, tag_name = None):
		if tag_id is not None:
			tag = self.get_tag(tag_id = tag_id)
		elif tag_name is not None:
			tag = self.get_tag(tag_name = tag_name)

		return bool(tag['Ctrl'])
