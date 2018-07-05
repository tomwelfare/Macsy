from pymongo import MongoClient
import urllib.parse
from macsy.blackboard import Blackboard
from macsy.date_based_blackboard import DateBasedBlackboard

__all__ = ['BlackboardAPI']

class BlackboardAPI():

	__client = None
	__db = None
	__admin_mode = False
	__admin_user = 'dbadmin'
	__salt = urllib.parse.quote_plus(')Djmsn)p')
	__read_primaries = False
	__protected_names = ['ARTICLE','FEED','OUTLET','TWEET','URL','MODULE','MODULE_RUN']

	def __init__(self, settings, admin_mode=False, read_primaries=False):
		if self._valid_settings(settings):
			self.__read_primaries = read_primaries
			self.__admin_mode = self._check_admin_attempt(settings, admin_mode)
			connection_str = self._parse_connection_string(settings, read_primaries)	
			self.__client = MongoClient(connection_str)
			self.__db = self.__client[settings.dbname]

	def load_blackboard(self, blackboard_name, date_based=None):
		if self._valid_blackboard_name(blackboard_name):
			if date_based is None:
				date_based = Blackboard.counter_type_date_based == self.get_blackboard_type(self.__db, blackboard_name)
			if date_based:
				return DateBasedBlackboard(self.__db, blackboard_name, self.__admin_mode)
			else:
				return Blackboard(self.__db, blackboard_name, self.__admin_mode)

	def drop_blackboard(self, blackboard_name):
		''' Drop a blackboard from the database, use with caution!'''
		if self._valid_blackboard_name(blackboard_name):
			if blackboard_name.upper() in self.__protected_names and not self.__admin_mode:
				raise PermissionError('Protected blackboards cannot be dropped without admin privileges.')
				return False
			else:
				# TODO: Deal with the case of dropping every year from a date-based blackboard
				return self.__db.drop_collection(blackboard_name)

	def get_blackboard_type(self, blackboard_name, blackboard_type = Blackboard.counter_type_standard):
		collection = self.__db[blackboard_name + '_COUNTER']
		result = collection.find_one({'_id' : Blackboard.counter_type})
		if result:
			blackboard_type = result.get(Blackboard.counter_type)
		return blackboard_type
		
	def _valid_settings(self, settings):
		''' Validate the settings input by the user, checking if the right fields are present.'''
		valid = settings is not None
		for key in ['user', 'password', 'dbname', 'dburl']:
			valid = key in settings
		if not valid:
			raise ValueError('Incorrect or incomplete database settings supplied.')
		return valid

	def _valid_blackboard_name(self, blackboard_name):
		''' Valid the blackboard_name input by the user, checking if it contains forbidden characters.'''
		if '$' in blackboard_name or ' ' in blackboard_name or '_' in blackboard_name:
			raise ValueError('Forbidden characters in blackboard name (\'$\',\'_\',\' \')')
			return False
		return True

	def _parse_connection_string(self, settings, read_primaries):
		''' Parse the connection string details from the settings object, to be passed to the MongoClient'''
		dbuser = urllib.parse.quote_plus(settings.user)
		dbpass = urllib.parse.quote_plus(settings.password)
		dbname = settings.dbname
		dburl = settings.dburl.replace('mongodb://','').strip('/')
		read_pref = '?readPreference=secondary'
		if read_primaries is True:
			read_pref = ''
		return 'mongodb://%s:%s@%s/%s%s' % (dbuser, dbpass, dburl, dbname, read_pref)


	def _check_admin_attempt(self, settings, admin_mode):
		''' Check if the user has rights to run in admin_mode, and salt their password if not'''
		if settings.user == self.__admin_user:
			return admin_mode
		else:
			settings.password += str(self.__salt)
			return False
