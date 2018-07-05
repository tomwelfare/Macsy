__all__ = ['']

class DateBasedBlackboard(Blackboard):

	# db objects and settings
	__db = None
	__name = None
	__admin_mode = False

	__document_collections = {}
	__tag_collection = None
	__counter_collection = None

	def __init__(self, database, blackboard_name, admin_mode=False):
		__db = database
		__name = blackboard_name
		__admin_mode = admin_mode
		__document_collections[0] = __db[blackboard_name]
		__tag_collection = __db[blackboard_name + '_TAGS']
		__counter_collection = __db[blackboard_name + '_COUNTER']