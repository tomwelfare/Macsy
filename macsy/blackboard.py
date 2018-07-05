

__all__ = ['']

class Blackboard():

	#Error codes
	_error_doc_not_found = None
	_error_doc_exists = -1
	_error_tag_not_found = 0
	_error_tag_exists = -1

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

	# db objects and settings
	__db = None
	__name = None
	__admin_mode = False

	__document_collection = None
	__tag_collection = None
	__counter_collection = None

	def __init__(self, database, blackboard_name, admin_mode=False):
		__db = database
		__name = blackboard_name
		__admin_mode = admin_mode
		__document_collection = __db[blackboard_name]
		__tag_collection = __db[blackboard_name + '_TAGS']
		__counter_collection = __db[blackboard_name + '_COUNTER']

	def get_type(database, blackboard_name, blackboard_type = counter_type_standard):
		collection = database[blackboard_name + '_COUNTER']
		result = collection.find_one({'_id' : counter_type})
		if result:
			blackboard_type = result.get(counter_type)
		return blackboard_type


