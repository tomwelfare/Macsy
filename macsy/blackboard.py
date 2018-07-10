import pymongo
from macsy.blackboard_cursor import BlackboardCursor
from dateutil import parser as dtparser
from bson.objectid import ObjectId

__all__ = ['Blackboard']

class Blackboard():

	# Predefined tags and fields
	doc_id = '_id'
	doc_tags = 'Tg'
	doc_control_tags = 'FOR'

	tag_suffix = '_TAGS'
	tag_id = '_id'
	tag_name = 'Nm'
	tag_control = 'Ctrl'
	tag_inherit = 'DInh'
	tag_control_for = 'FOR>'
	tag_control_post = 'POST>'

	counter_suffix = '_COUNTER'
	counter_id = '_id'
	counter_next = 'NEXT_ID'
	counter_type = 'BLACKBOARD_TYPE'
	counter_type_standard = 'STANDARD'
	counter_type_date_based = 'DATE_BASED'

	def __init__(self, settings):
		self._db, self._name, self._admin_mode = settings
		self._document_collection = self._db[self._name]
		self._tag_collection = self._db[self._name + Blackboard.tag_suffix]
		self._counter_collection = self._db[self._name + Blackboard.counter_suffix]

	def count(self, **kwargs):
		query = kwargs.get('query', self._build_query(**kwargs))
		return self._document_collection.find(query).count()

	def find(self, **kwargs):
		settings = (kwargs.get('query', self._build_query(**kwargs)), 
			kwargs.pop('max', 0), 
			[(Blackboard.doc_id, kwargs.pop('sort', pymongo.DESCENDING))])
		result = self._get_result(settings)
		return BlackboardCursor(result)

	def get_tag(self, tag_id = None, tag_name = None):
		if tag_id is not None:
			return self._tag_collection.find_one({Blackboard.tag_id : tag_id})
		if tag_name is not None:
			return self._tag_collection.find_one({Blackboard.tag_name : tag_name})

	def is_control_tag(self, tag_id = None, tag_name = None):
		return self.__tag_has_property(Blackboard.tag_control, tag_id, tag_name)

	def is_inheritable_tag(self, tag_id = None, tag_name = None):
		return self.__tag_has_property(Blackboard.tag_inherit, tag_id, tag_name)
		
	def get_date(self, doc):
		return doc[Blackboard.doc_id].generation_time

	def _get_result(self, qms):
		return self._document_collection.find(qms[0]).limit(qms[1]).sort(qms[2])

	def _build_query(self, **kwargs):
		qw = {'tags' : ('$all', self.__build_tag_query, {}), 
			'without_tags' : ('$nin', self.__build_tag_query, {}), 
			'fields' : (True, self.__build_field_query, {}), 
			'without_fields' : (False, self.__build_field_query, {}), 
			'min_date' : ('$gte', self.__build_date_query, None), 
			'max_date' : ('$lt', self.__build_date_query, None)}
		query = {}
		for k in set(kwargs).intersection(qw):
			for d in kwargs.get(k,qw[k][2]):
				assert type(kwargs.get(k,qw[k][2])) is list, \
				'Argument needs to be a list: {}'.format(kwargs.get(k, qw[k][2]))
				key, value = qw[k][1]((query, d, qw[k][0]))
				query[key] = value
		return query

	def __build_date_query(self, qdv):
		q = qdv[0].get(Blackboard.doc_id, {})
		q[qdv[2]] = ObjectId.from_datetime(dtparser.parse(str(qdv[1])))
		return Blackboard.doc_id, q

	def __build_tag_query(self, qtv):
		full_tag = self.__get_canonical_tag(qtv[1])
		is_ctrl_tag = Blackboard.tag_control in full_tag and full_tag[Blackboard.tag_control]
		field = Blackboard.doc_control_tags if is_ctrl_tag else Blackboard.doc_tags
		if field in qtv[0] and '$exists' in qtv[0][field]: del qtv[0][field]
		q = qtv[0].get(field, {qtv[2] : [int(full_tag[Blackboard.tag_id])]})
		if int(full_tag[Blackboard.tag_id]) not in q[qtv[2]]:
			q[qtv[2]].append(int(full_tag[Blackboard.tag_id]))
		return field, q

	def __build_field_query(self, qfv):
		return qfv[1], qfv[0].get(qfv[1], {'$exists' : qfv[2]})

	def __get_canonical_tag(self, tag):
		full_tag = self.get_tag(tag_name=tag) if type(tag) is str else self.get_tag(tag_id=tag)
		if full_tag is None:
			raise ValueError('Tag does not exist: {}'.format(tag))
		return full_tag

	def __tag_has_property(self, tag_property, tag_id = None, tag_name = None):
		tag = self.get_tag(tag_id = tag_id) if tag_id is not None else self.get_tag(tag_name = tag_name)
		test = tag[tag_property] if (tag is not None and tag_property in tag) else False
		return bool(test)
