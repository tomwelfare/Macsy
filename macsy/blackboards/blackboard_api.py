from pymongo import MongoClient
import urllib.parse
from macsy.blackboards import blackboard, date_based_blackboard
Blackboard = blackboard.Blackboard
DateBasedBlackboard = date_based_blackboard.DateBasedBlackboard

class BlackboardAPI():

	__setting_fields = {'username' : 'user', 'password' : 'password', 'dbname' : 'dbname', 'dburl' : 'dburl'}
	__protected_names = ['ARTICLE','FEED','OUTLET','TWEET','URL','MODULE','MODULE_RUN', 'Newspapers', 'AmericanNews']
	__admin_user = 'dbadmin'
	__salt = ')Djmsn)p'

	def _validate_blackboard_name(fn):
		''' Validate the blackboard_name input by the user, checking if it contains forbidden characters.'''
		def wrap(*args, **kwargs):
			if any(x in args[1] for x in r" \$_"):
				raise ValueError('Forbidden characters in blackboard name ("$","_"," ")')
			return fn(*args, **kwargs)
		return wrap

	def _validate_settings(fn):
		'''	Validate the settings input by the user, checking if the right fields are present.'''
		def wrap(*args, **kwargs):
			required_fields = BlackboardAPI.__setting_fields.values()
			if len(set(required_fields).intersection(args[1])) is not len(required_fields):
				raise ValueError('Incorrect or incomplete database settings supplied.')
			return fn(*args, **kwargs)
		return wrap

	@_validate_settings
	def __init__(self, settings, MongoClient=MongoClient):
		self.__username = urllib.parse.quote_plus(settings[BlackboardAPI.__setting_fields.get('username')])
		self.__password = urllib.parse.quote_plus(settings[BlackboardAPI.__setting_fields.get('password')])
		self.__dbname = settings[BlackboardAPI.__setting_fields.get('dbname')]
		self.__dburl = settings[BlackboardAPI.__setting_fields.get('dburl')].replace('mongodb://','').strip('/')
		self.__admin_mode = self._check_admin_attempt(settings)
		self.__client = MongoClient(self._parse_connection_string(settings))
		self.__db = self.__client[self.__dbname]

	@_validate_blackboard_name
	def blackboard_exists(self, blackboard_name):
		collection = self.__db[blackboard_name + Blackboard.counter_suffix]
		result = collection.find_one({Blackboard.counter_id : Blackboard.counter_type})
		if result:
			return True
		return False

	@_validate_blackboard_name
	def load_blackboard(self, blackboard_name, date_based=None):
		settings = (self.__db, blackboard_name, self.__admin_mode)
		return DateBasedBlackboard(settings) if self.get_blackboard_type(blackboard_name, date_based) == Blackboard.counter_type_date_based else Blackboard(settings)

	@_validate_blackboard_name
	def drop_blackboard(self, blackboard_name):
		''' Drop a blackboard from the database, use with caution!'''
		protected = blackboard_name.upper() in BlackboardAPI.__protected_names
		if protected and not self.__admin_mode:
			raise PermissionError('Protected blackboards cannot be dropped without admin privileges.')
		else:
			drop_method = {Blackboard.counter_type_standard : self.__drop_standard_blackboard,
				Blackboard.counter_type_date_based : self.__drop_date_based_blackboard}
			drop_method[self.get_blackboard_type(blackboard_name)](blackboard_name)

	@_validate_blackboard_name
	def get_blackboard_type(self, blackboard_name, date_based=None):
		collection = self.__db[blackboard_name + Blackboard.counter_suffix]
		result = collection.find_one({Blackboard.counter_id : Blackboard.counter_type})
		if result is not None:
			self.__check_blackboard_type_errors((blackboard_name, result.get(Blackboard.counter_type), date_based))
			return result.get(Blackboard.counter_type)
		return Blackboard.counter_type_date_based if date_based else Blackboard.counter_type_standard
		
	def _parse_connection_string(self, settings):
		''' Parse the connection string details from the settings object, to be passed to the MongoClient.'''
		return 'mongodb://%s:%s@%s/%s%s' % (self.__username, self.__password, self.__dburl, self.__dbname, '?readPreference=secondary')


	def _check_admin_attempt(self, settings):
		'''	Check if the user has rights to run in admin_mode, and salt their password if not'''
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
