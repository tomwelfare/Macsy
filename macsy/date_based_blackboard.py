from macsy.blackboard import Blackboard

__all__ = ['DateBasedBlackboard']

class DateBasedBlackboard(Blackboard):

	# db objects and settings
	__db = None
	__name = None
	__admin_mode = False

	__document_collections = {}
	__tag_collection = None
	__counter_collection = None

	def __init__(self, database, blackboard_name, admin_mode=False):
		self.__db = database
		self.__name = blackboard_name
		self.__admin_mode = admin_mode
		self.__document_collections[0] = self.__db[blackboard_name]
		self.__tag_collection = self.__db[blackboard_name + '_TAGS']
		self.__counter_collection = self.__db[blackboard_name + '_COUNTER']
