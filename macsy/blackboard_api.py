from pymongo import MongoClient
import urllib.parse
from macsy.blackboard import Blackboard
from macsy.date_based_blackboard import DateBasedBlackboard

__all__ = ['BlackboardAPI']

class BlackboardAPI():

	__protected_names = ['ARTICLE','FEED','OUTLET','TWEET','URL','MODULE','MODULE_RUN']
	__admin_user = 'dbadmin'
	__salt = ')Djmsn)p'

	def __init__(self, settings, MongoClient=MongoClient):
		if self._valid_settings(settings):
			self.__admin_mode = self._check_admin_attempt(settings)
			self.__client = MongoClient(self._parse_connection_string(settings))
			self.__db = self.__client[settings['dbname']]

	def load_blackboard(self, blackboard_name, date_based=None):
		if self._valid_blackboard_name(blackboard_name):
			settings = (self.__db, blackboard_name, self.__admin_mode)
			return DateBasedBlackboard(settings) \
			if self.get_blackboard_type(blackboard_name, date_based)\
			== Blackboard.counter_type_date_based \
			else Blackboard(settings)

	def drop_blackboard(self, blackboard_name):
		''' Drop a blackboard from the database, use with caution!'''
		if self._valid_blackboard_name(blackboard_name):
			if blackboard_name.upper() in BlackboardAPI.__protected_names and not self.__admin_mode:
				raise PermissionError('Protected blackboards cannot be dropped without admin privileges.')
				return False
			else:
				# TODO: Deal with the case of dropping every year from a date-based blackboard
				return self.__db.drop_collection(blackboard_name)

	def get_blackboard_type(self, blackboard_name, date_based=None):
		if date_based is not None:
			return Blackboard.counter_type_date_based if date_based else Blackboard.counter_type_standard

		collection = self.__db[blackboard_name + '_COUNTER']
		result = collection.find_one({'_id' : Blackboard.counter_type})
		return result.get(Blackboard.counter_type) if result is not None else None
		
	def _valid_settings(self, settings):
		'''
		Validate the settings input by the user, checking if the right fields 
		are present.
		'''
		required_fields = ['user', 'password', 'dbname', 'dburl']
		if len(set(required_fields).intersection(settings)) is not len(required_fields):
			raise ValueError('Incorrect or incomplete database settings supplied.')
		return True

	def _valid_blackboard_name(self, blackboard_name):
		'''
		Valid the blackboard_name input by the user, checking if it contains 
		forbidden characters.
		'''
		if any(x in blackboard_name for x in r" \$_"):
			raise ValueError('Forbidden characters in blackboard name ("$","_"," ")')
			return False
		return True

	def _parse_connection_string(self, settings):
		''' 
		Parse the connection string details from the settings object, to be 
		passed to the MongoClient.
		'''
		dbuser = urllib.parse.quote_plus(settings['user'])
		dbpass = urllib.parse.quote_plus(settings['password'])
		dbname = settings['dbname']
		dburl = settings['dburl'].replace('mongodb://','').strip('/')
		read_pref = '?readPreference=secondary'
		return 'mongodb://%s:%s@%s/%s%s' % (dbuser, dbpass, dburl, dbname, read_pref)


	def _check_admin_attempt(self, settings):
		'''
		Check if the user has rights to run in admin_mode, and salt their
		password if not
		'''
		settings['password'] += str(BlackboardAPI.__salt) if settings['user'] is not BlackboardAPI.__admin_user else ''
		return True if settings['user'] == BlackboardAPI.__admin_user else False
