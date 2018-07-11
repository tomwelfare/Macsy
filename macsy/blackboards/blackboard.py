import pymongo
from bson.objectid import ObjectId
from dateutil import parser as dtparser
from macsy.blackboards import blackboard_cursor
BlackboardCursor = blackboard_cursor.BlackboardCursor

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

	def _check_admin(error):
		def dec(fn):
			def wrap(*args, **kwargs):
				self = args[0]
				if not self._admin_mode:
					raise PermissionError(error)
				return fn(*args, **kwargs)
			return wrap
		return dec

	def count(self, **kwargs):
		query = kwargs.get('query', self._build_query(**kwargs))
		return self._document_collection.find(query).count()

	def find(self, **kwargs):
		settings = (kwargs.get('query', self._build_query(**kwargs)), 
			kwargs.pop('max', 0), 
			[(Blackboard.doc_id, kwargs.pop('sort', pymongo.DESCENDING))])
		result = self._get_result(settings)
		return BlackboardCursor(result)

	# TODO: Later on, need to add bulk insert/update/delete methods
	def insert(self, doc):
		raise NotImplementedError()

	def update(self, doc_id, doc):
		raise NotImplementedError()

	@_check_admin('Admin rights required to delete documents.')
	def delete(self, doc_id):
		return self._document_collection.remove({Blackboard.doc_id : doc_id})

	def insert_tag(self, tag_id, tag_name, control = False, inheritable = False):
		raise NotImplementedError()

	def update_tag(self, tag_id, tag_name, control = None, inheritable = None):
		raise NotImplementedError()

	@_check_admin('Admin rights required to delete tags.')
	def delete_tag(self, tag_id):
		self._remove_tag_from_all(tag_id)
		return self._tag_collection.remove({Blackboard.tag_id : tag_id})

	def add_tag(self, doc_id, tag_id):
		return self._add_remove_tag((doc_id, tag_id), '$addToSet')
	
	def remove_tag(self, doc_id, tag_id):
		return self._add_remove_tag((doc_id, tag_id), '$pull')

	def get_tag(self, tag_id = None, tag_name = None):
		if tag_id is not None:
			return self._tag_collection.find_one({Blackboard.tag_id : tag_id})
		if tag_name is not None:
			return self._tag_collection.find_one({Blackboard.tag_name : tag_name})

	def is_control_tag(self, tag_id = None, tag_name = None):
		return self._tag_has_property(Blackboard.tag_control, tag_id, tag_name)

	def is_inheritable_tag(self, tag_id = None, tag_name = None):
		return self._tag_has_property(Blackboard.tag_inherit, tag_id, tag_name)

	def _get_result(self, qms):
		query, max_docs, sort = qms
		return self._document_collection.find(query).limit(max_docs).sort(sort)

	def _build_query(self, **kwargs):
		qw = {'tags' : ('$all', self._build_tag_query, {}), 
			'without_tags' : ('$nin', self._build_tag_query, {}), 
			'fields' : (True, self._build_field_query, {}), 
			'without_fields' : (False, self._build_field_query, {}), 
			'min_date' : ('$gte', self._build_date_query, None), 
			'max_date' : ('$lt', self._build_date_query, None)}
		query = {}
		for k in set(kwargs).intersection(qw):
			for d in kwargs.get(k,qw[k][2]):
				assert type(kwargs.get(k,qw[k][2])) is list, \
				'Argument needs to be a list: {}'.format(kwargs.get(k, qw[k][2]))
				key, value = qw[k][1]((query, d, qw[k][0]))
				query[key] = value

		return query

	def _build_date_query(self, qdv):
		query, date, value = qdv
		q = query.get(Blackboard.doc_id, {})
		q[value] = ObjectId.from_datetime(dtparser.parse(str(date)))
		return Blackboard.doc_id, q

	def _build_tag_query(self, qtv):
		query, tag, value = qtv
		full_tag = self._get_canonical_tag(tag)
		field = Blackboard.doc_control_tags if (Blackboard.tag_control in full_tag and full_tag[Blackboard.tag_control]) else Blackboard.doc_tags
		if field in query and '$exists' in query[field]: del query[field]
		q = query.get(field, {value : [int(full_tag[Blackboard.tag_id])]})
		if int(full_tag[Blackboard.tag_id]) not in q[value]:
			q[value].append(int(full_tag[Blackboard.tag_id]))
		return field, q

	def _build_field_query(self, qfv):
		query, field, value = qfv
		return field, query.get(field, {'$exists' : value})

	def _get_canonical_tag(self, tag):
		full_tag = self.get_tag(tag_name=tag) if type(tag) is str else self.get_tag(tag_id=tag)
		if full_tag is None:
			raise ValueError('Tag does not exist: {}'.format(tag))
		return full_tag

	def _tag_has_property(self, tag_property, tag_id = None, tag_name = None):
		tag = self.get_tag(tag_id = tag_id) if tag_id is not None else self.get_tag(tag_name = tag_name)
		test = tag[tag_property] if (tag is not None and tag_property in tag) else False
		return bool(test)

	def _add_remove_tag(self, ids, operation):
		doc_id, tag_id = ids
		self._validate_tag(tag_id)
		field = Blackboard.doc_control_tags if self.is_control_tag(tag_id) else Blackboard.doc_tags
		return self._document_collection.update({Blackboard.doc_id : doc_id}, {operation, {field:  tag_id}})

	@_check_admin('Admin rights required to remove a tag from all documents.')
	def _remove_tag_from_all(self, tag_id):
		print('Removing tag {} from {} documents.'.format(tag_id, self.count(tags=[tag_id])))
		for doc in self.find(tags=[tag_id]):
			self.remove_tag(doc[Blackboard.doc_id], tag_id)