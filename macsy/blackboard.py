import pymongo
from macsy.blackboard_cursor import BlackboardCursor
from dateutil import parser as dtparser
from bson.objectid import ObjectId

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

	def count(self, **kwargs):
		query = kwargs.get('query', self._build_query(**kwargs))
		return self._document_collection.find(query).count()

	def find(self, **kwargs):
		max_docs = kwargs.pop('max', 0)
		sort = [(Blackboard.doc_id, kwargs.pop('sort', pymongo.DESCENDING))]
		query = kwargs.get('query', self._build_query(**kwargs))
		result = self._get_result(query, max_docs, sort)
		return BlackboardCursor(result)

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

		if tag:
			return bool(tag['Ctrl'])
		else: 
			return False

	def get_date(self, doc):
		return doc[Blackboard.doc_id].generation_time

	def _get_result(self, query, max_docs, sort):
		return self._document_collection.find(query).limit(max_docs).sort(sort)

	def _build_query(self, **kwargs):
		qw = {'tags' : ('$all', self.__build_tag_query, {}), 
			'without_tags' : ('$nin', self.__build_tag_query, {}), 
			'fields' : (True, self.__build_field_query, {}), 
			'without_fields' : (False, self.__build_field_query, {}), 
			'min_date' : ('$gte', self.__build_date_query, None), 
			'max_date' : ('$lt', self.__build_date_query, None)}
		query = {}
		for k in set(qw).intersection(kwargs):
			for d in kwargs.get(k,qw[k][2]):
				assert type(kwargs.get(k,qw[k][2])) is list, \
				'Argument needs to be a list: {}'.format(kwargs.get(k, qw[k][2]))
				key, value = qw[k][1](query, d, qw[k][0])
				query[key] = value

		print(query)
		return query

	def __build_date_query(self, query, date, value):
		field = Blackboard.doc_id
		dt_obj = dtparser.parse(str(date))
		obj_id = ObjectId.from_datetime(dt_obj)
		q = query.get(field, {})
		q[value] = obj_id
		return field, q

	def __build_tag_query(self, query, tag, value):
		if type(tag) is str:
			full_tag = self.get_tag(tag_name=tag)
		else:
			full_tag = self.get_tag(tag_id=tag)

		field = 'Tg'
		if 'Ctrl' in full_tag and full_tag['Ctrl']:
			field = 'FOR'			

		q = query.get(field, {value : []})
		if value in q:
			q[value].append(int(full_tag['_id']))
		else:
			q[value] = [int(full_tag['_id'])]

		return field, q

	def __build_field_query(self, query, field, value):
		q = query.get(field, {'$exists' : value})
		return field, q
