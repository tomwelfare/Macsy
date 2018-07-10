from pymongo import MongoClient
import urllib.parse
from macsy.blackboard import Blackboard
from macsy.date_based_blackboard import DateBasedBlackboard

__all__ = ['BlackboardAPI']

class BlackboardAPI():

	__setting_fields = {'username' : 'user', 'password' : 'password', 'dbname' : 'dbname', 'dburl' : 'dburl'}
	__protected_names = ['ARTICLE','FEED','OUTLET','TWEET','URL','MODULE','MODULE_RUN']
	__admin_user = 'dbadmin'
	__salt = ')Djmsn)p'

	def __init__(self, settings, MongoClient=MongoClient):
		if self._valid_settings(settings):
			self.__username = urllib.parse.quote_plus(settings[BlackboardAPI.__setting_fields.get('username')])
			self.__password = urllib.parse.quote_plus(settings[BlackboardAPI.__setting_fields.get('password')])
			self.__dbname = settings[BlackboardAPI.__setting_fields.get('dbname')]
			self.__dburl = settings[BlackboardAPI.__setting_fields.get('dburl')].replace('mongodb://','').strip('/')
			self.__admin_mode = self._check_admin_attempt(settings)
			self.__client = MongoClient(self._parse_connection_string(settings))
			self.__db = self.__client[self.__dbname]

	def blackboard_exists(self, blackboard_name):
		if self._valid_blackboard_name(blackboard_name):
			collection = self.__db[blackboard_name + Blackboard.counter_suffix]
			result = collection.find_one({Blackboard.counter_id : Blackboard.counter_type})
			if result:
				return True
		return False

	def load_blackboard(self, blackboard_name, date_based=None):
		if self._valid_blackboard_name(blackboard_name):
			settings = (self.__db, blackboard_name, self.__admin_mode)
			return DateBasedBlackboard(settings) if self.get_blackboard_type(blackboard_name, date_based) == Blackboard.counter_type_date_based else Blackboard(settings)

	def drop_blackboard(self, blackboard_name):
		''' Drop a blackboard from the database, use with caution!'''
		if self._valid_blackboard_name(blackboard_name):
			protected = blackboard_name.upper() in BlackboardAPI.__protected_names
			if protected and not self.__admin_mode:
				raise PermissionError('Protected blackboards cannot be dropped without admin privileges.')
			else:
				drop_method = {Blackboard.counter_type_standard : self.__drop_standard_blackboard,
					Blackboard.counter_type_date_based : self.__drop_date_based_blackboard}
				drop_method[self.get_blackboard_type(blackboard_name)](blackboard_name)

	def get_blackboard_type(self, blackboard_name, date_based=None):
		collection = self.__db[blackboard_name + Blackboard.counter_suffix]
		result = collection.find_one({Blackboard.counter_id : Blackboard.counter_type})
		if result is not None:
			self.__check_blackboard_type_errors((blackboard_name, result.get(Blackboard.counter_type), date_based))
			return result.get(Blackboard.counter_type)
		return Blackboard.counter_type_date_based if date_based else Blackboard.counter_type_standard
		
	# Should implement as a decorator?
	def _valid_settings(self, settings):
		'''
		Validate the settings input by the user, checking if the right fields 
		are present.
		'''
		required_fields = BlackboardAPI.__setting_fields.values()
		if len(set(required_fields).intersection(settings)) is not len(required_fields):
			raise ValueError('Incorrect or incomplete database settings supplied.')
		return True

	# Should implement as a decorator?
	def _valid_blackboard_name(self, blackboard_name):
		'''
		Valid the blackboard_name input by the user, checking if it contains 
		forbidden characters.
		'''
		if any(x in blackboard_name for x in r" \$_"):
			raise ValueError('Forbidden characters in blackboard name ("$","_"," ")')
		return blackboard_name

	def _parse_connection_string(self, settings):
		''' 
		Parse the connection string details from the settings object, to be 
		passed to the MongoClient.
		'''
		return 'mongodb://%s:%s@%s/%s%s' % (self.__username, self.__password, self.__dburl, self.__dbname, '?readPreference=secondary')


	def _check_admin_attempt(self, settings):
		'''
		Check if the user has rights to run in admin_mode, and salt their
		password if not
		'''
		self.__password += str(BlackboardAPI.__salt) if self.__username is not BlackboardAPI.__admin_user else ''
		return True if self.__username == BlackboardAPI.__admin_user else False

	def __drop_standard_blackboard(self, blackboard_name):
		for suffix in ['', Blackboard.counter_suffix, Blackboard.tag_suffix]:
			self.__db.drop_collection(blackboard_name + suffix)

	def __drop_date_based_blackboard(self, blackboard_name):
		for coll in self.__db.collection_names():
			suffix = coll.split('_')[-1]
			if blackboard_name == coll.split('_')[0] and (suffix.isdigit() or suffix in Blackboard.counter_suffix or Blackboard.tag_suffix):
				self.__db.drop_collection(coll)

	def __check_blackboard_type_errors(self, ntd):
		standard = ntd[1] == Blackboard.counter_type_standard
		atype = Blackboard.counter_type_standard if standard else Blackboard.counter_type_date_based
		if ntd[2] == standard:
			raise ValueError('{} is a {} blackboard.'.format(ntd[0], atype))
