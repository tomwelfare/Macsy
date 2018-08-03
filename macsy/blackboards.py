'''Blackboards are objects which provide an interface to data stored in the database.'''

from functools import wraps, partial
import urllib.parse
from pymongo import MongoClient
from macsy.cursors import BlackboardCursor
from macsy.managers import TagManager, DocumentManager, DateBasedDocumentManager, CounterManager

def check_admin(error):
    '''Decorator to validate the if the user is admin or not.'''
    def dec(func):
        @wraps(func)
        def wrap(*args, **kwargs):
            if not args[0].admin_mode:
                raise PermissionError(error)
            return func(*args, **kwargs)
        return wrap
    return dec

def validate_blackboard_name(func):
    '''Decorator to validate the blackboard_name, checking if it contains forbidden characters.'''
    @wraps(func)
    def wrap(*args, **kwargs):
        if any(x in args[1] for x in r" \$_"):
            raise ValueError('Forbidden characters in blackboard name ("$","_"," ")')
        return func(*args, **kwargs)
    return wrap

def validate_settings(func):
    '''Decorator to validate the settings, checking if the right fields are present.'''
    @wraps(func)
    def wrap(*args, **kwargs):
        required_fields = BlackboardAPI._setting_fields.values()
        if len(set(required_fields).intersection(args[1])) is not len(required_fields):
            raise ValueError('Incorrect or incomplete database settings supplied.')
        return func(*args, **kwargs)
    return wrap

class BlackboardAPI():
    '''Entry object for loading and deleting blackboards.

    Example:
        >>> settings = {'username' : 'user', 'password' : 'password', 'dbname' : 'database', 'dburl' : 'localhost:37017'}
        >>> api = blackboard_api.BlackboardAPI(settings)
        >>> blackboards_available = api.get_blackboard_names()
        >>> blackboard = api.load_blackboard('ARTICLE')
        >>> blackboard.count()
    '''

    _setting_fields = {'username' : 'user', 'password' : 'password', 'dbname' : 'dbname', 'dburl' : 'dburl'}
    _protected_names = ['ARTICLE', 'FEED', 'OUTLET', 'TWEET', 'URL', 'MODULE', 'MODULE_RUN', 'Newspapers', 'AmericanNews']
    _admin_user = 'dbadmin'
    _salt = ')Djmsn)p'

    @validate_settings
    def __init__(self, settings, MongoClient=MongoClient):
        '''Constructor for the BlackboardAPI.

        Args:
            settings (:class:`dict`): dictionary containing the database settings to use, including
                username, password, dbname (database name) and dburl (database url).
            MongoClient (:class:`MongoClient`, optional): optional :class:`MongoClient` to use, generally
                useful for mocking, and testing the blackboards without connecting to a real
                database.

        Raises:
            :class:`ValueError`: If incorrect or incomplete database settings are provided.
        '''
        self.__username = urllib.parse.quote_plus(settings[
            BlackboardAPI._setting_fields.get('username')])
        self.__password = urllib.parse.quote_plus(settings[
            BlackboardAPI._setting_fields.get('password')])
        self.__dbname = settings[
            BlackboardAPI._setting_fields.get('dbname')]
        self.__dburl = settings[
            BlackboardAPI._setting_fields.get('dburl')].replace('mongodb://', '').strip('/')
        self.__admin_mode = self._check_admin_attempt(settings)
        self.__client = MongoClient(self._get_connection_string(settings))
        self.__db = self.__client[self.__dbname]

    def get_blackboard_names(self):
        '''Retrieve a list of all available blackboard names.

        Returns:
            :class:`list[str]`: names of available blackboards.
        '''
        suffix_len = len(CounterManager.counter_suffix)
        collections = self.__db.collection_names(include_system_collections=False)
        blackboards = (coll[0:-suffix_len] for coll in collections if coll.endswith(CounterManager.counter_suffix))
        blackboards = [blackboard for blackboard in blackboards if self.blackboard_exists(blackboard.split('_')[0])]

        return blackboards

    @validate_blackboard_name
    def blackboard_exists(self, blackboard_name):
        '''Check by name if a blackboard exists.

        Args:
            blackboard_name (:class:`str`): the blackboard name to check for existence.

        Returns:
           :class:`bool`: True if blackboard exists, False otherwise.

        Raises:
            :class:`ValueError`: If **blackboard_name** contains forbidden characters.
        '''
        collection = self.__db[blackboard_name + CounterManager.counter_suffix]
        result = collection.find_one({CounterManager.counter_id : CounterManager.counter_type})
        if result:
            return True
        return False

    @validate_blackboard_name
    def load_blackboard(self, blackboard_name, date_based=None):
        '''Load or create (if it doesn't exist) a blackboard by name and return it.

        Args:
            blackboard_name (:class:`str`): the name of the blackboard to load or create.

            date_based (:class:`bool`, optional): whether or not the blackboard should be date-based or not.

        Returns:
            :class:`Blackboard`

        Raises:
            :class:`ValueError`: If **blackboard_name** contains forbidden characters.
        '''
        settings = (self.__db, blackboard_name, self.__admin_mode)
        return DateBasedBlackboard(settings) \
            if self.get_blackboard_type(blackboard_name, date_based) == \
            CounterManager.counter_type_date_based else Blackboard(settings)

    @validate_blackboard_name
    def drop_blackboard(self, blackboard_name):
        '''Drop (delete) a blackboard from the database.

        .. danger:: This will delete all data in the database! Data loss will occur if you drop a blackboard! 

        Protected blackboards require admin permissions in order to remove them,
        to guard against the accidental deletion of the important data in the database.

        Args:
            blackboard_name (:class:`str`): the name of the blackboard to delete.

        Raises:
            :class:`ValueError`: If **blackboard_name** contains forbidden characters.
            :class:`PermissionError`: If **blackboard_name** is a protected blackboard, and the user does not have admin privileges.
        '''
        protected = blackboard_name.upper() in BlackboardAPI._protected_names
        if protected and not self.__admin_mode:
            raise PermissionError(
                'Protected blackboards cannot be dropped without admin privileges.' +
                ' Attempt has been logged.')
        else:
            drop_method = {CounterManager.counter_type_standard : self.__drop_standard_blackboard,\
                CounterManager.counter_type_date_based : self.__drop_date_based_blackboard}
            blackboard_type = self.get_blackboard_type(blackboard_name)
            drop_method[blackboard_type](blackboard_name)

    @validate_blackboard_name
    def get_blackboard_type(self, blackboard_name, date_based=None):
        '''Get the type of the blackboard and return it as a string.

        Args:
            blackboard_name (:class:`str`): the name of the blackboard to check.
            date_based (:class:`bool`, optional): whether the blackboard is date-based or not.

        Returns:
            :class:`str`: "DATEBASED" if blackboard is date-based, "STANDARD" otherwise.

        Raises:
            :class:`ValueError`: If **blackboard_name** contains forbidden characters.
        '''
        collection = self.__db[blackboard_name + CounterManager.counter_suffix]
        result = collection.find_one({CounterManager.counter_id : CounterManager.counter_type})
        if result is not None:
            BlackboardAPI._check_blackboard_type_errors((blackboard_name, \
                result.get(CounterManager.counter_type), date_based))
            return result.get(CounterManager.counter_type)
        types = {True: CounterManager.counter_type_date_based, False: CounterManager.counter_type_standard, None: None}
        return types[date_based]

    @validate_settings
    def _get_connection_string(self, settings):
        settings = (self.__username, self.__password, \
            self.__dburl, self.__dbname, '')#'?readPreference=secondary')
        return 'mongodb://%s:%s@%s/%s%s' % settings

    @validate_settings
    def _check_admin_attempt(self, settings):
        if self.__username != BlackboardAPI._admin_user:
            self.__password += str(BlackboardAPI._salt)
            return False
        return True

    def __drop_standard_blackboard(self, blackboard_name):
        for suffix in ['', CounterManager.counter_suffix, TagManager.tag_suffix]:
            self.__db.drop_collection(blackboard_name + suffix)

    def __drop_date_based_blackboard(self, blackboard_name):
        for coll in self.__db.collection_names():
            suffix = coll.split('_')[-1]
            if blackboard_name == coll.split('_')[0] and (suffix.isdigit() or \
                suffix in CounterManager.counter_suffix or TagManager.tag_suffix):
                self.__db.drop_collection(coll)

    @staticmethod
    def _check_blackboard_type_errors(ntd):
        standard = ntd[1] == CounterManager.counter_type_standard
        atype = CounterManager.counter_type_standard if standard else \
            CounterManager.counter_type_date_based
        if ntd[2] == standard:
            raise ValueError('{} is a {} blackboard.'.format(ntd[0], atype))


class Blackboard():
    '''Blackboard object that acts as an interface for retrieving and inserting data from a standard blackboard.

    Example:
        >>> blackboard = api.load_blackboard('FEED')
        >>> blackboard.count() # count total documents
        >>> for doc in blackboard.find():
        >>> ... print("{id} - {title}".format(id=doc['_id'], title=doc['T'])
    '''

    def __init__(self, settings):
        '''Constructor for Blackboard objects.

        This should not be called directly. Blackboards can be accessed by loading them using the BlackboardAPI.

        Example:
            >>> api = BlackboardAPI(settings)
            >>> blackboard = api.load_blackboard('FEED')
        '''
        self._db, self._name, self.admin_mode = settings
        self.document_manager = DocumentManager(self)
        self.tag_manager = TagManager(self)
        self.counter_manager = CounterManager(self)

    def count(self, **kwargs):
        '''Count the number of documents in the blackboard.

        Args:
            tags (:class:`list[int]`, optional): filter documents to those that have all of the specified tags.
            without_tags (:class:`list[int]`, optional): filter documents to those that do not have have any of specified tags.
            fields (:class:`list[str]`, optional): filter documents to those that have the specified fields.
            without_fields (:class:`list[str]`, optional): filter documents to those that do not have the specified fields.
            min_date (:class:`list[str]`, optional): filter documents to those that occur after the given date.
            max_date (:class:`list[str]`, optional): filter documents to those that occur before the given date.
            query (:class:`dict`): raw mongo query, bypassing other arguments.

        Returns:
            :class:`int`: number of documents in blackboard.
        '''
        return self.document_manager.count(**kwargs)

    def find(self, **kwargs):
        '''Return a cursor for documents in the blackboard.

        Args:
            tags (:class:`list[int]`, optional): filter documents to those that have all of the specified tags.
            without_tags (:class:`list[int]`, optional): filter documents to those that do not have have any of specified tags.
            fields (:class:`list[str]`, optional): filter documents to those that have the specified fields.
            without_fields (:class:`list[str]`, optional): filter documents to those that do not have the specified fields.
            min_date (:class:`list[str]`, optional): filter documents to those that occur after the given date.
            max_date (:class:`list[str]`, optional): filter documents to those that occur before the given date.
            query (:class:`dict`): raw mongo query, bypassing other arguments.

        Returns:
            :class:`BlackboardCursor`: cursor of results from the database.
        '''
        return BlackboardCursor(self.document_manager.find(**kwargs))

    
    def insert(self, doc):
        '''Insert a new document into the blackboard.

        Documents which do not contain an id field ('_id') will have a new id auto-generated for them.
        If the id for the document already exists, the document is updated with the new details (upsert).

        Args:
            doc (:class:`dict`): dictionary containing the fields and values to insert into the blackboard.

        Returns:
            :class:`ObjectId` or :class:`int`: id of the inserted document in the blackboard.
        '''
        return self.document_manager.insert(doc)

    def update(self, doc_id, updated_fields):
        '''Update an existing document in the blackboard.

        Documents which do not contain an id field ('_id') will have a new id auto-generated for them.
        If the id for the document already exists, the document is updated with the new details (upsert).

        Args:
            doc_id (:class:`int`): id of the document to update.
            updated_fields (:class:`dict`): dictionary containing the fields to be updated, along with their new values.

        Returns:
            :class:`ObjectId` or :class:`int` or :class:`None`: id of the updated document in the blackboard, or :class:`None` if id does not exist.
        '''
        return self.document_manager.update(doc_id, updated_fields)

    @check_admin('Admin rights required to delete documents.')
    def delete(self, doc_id):
        '''Delete a document from the blackboard.

        Documents can only be deleted by admin users.

        Args:
            doc_id (:class:`int`): id of the document to delete.

        Returns:
            ???

        Raises:
            :class:`PermissionError`: If the user does not have admin privileges.
        '''
        return self.document_manager.delete(doc_id)

    def get_all_tags(self):
        '''Get a list of all the tags in the blackboard.

        Returns:
            :class:`list[dict]`: list of tags, where each tag is a dictionary of the key-values from the blackboard.
        '''
        return self.tag_manager.get_all_tags()

    def add_tag(self, doc_id, tag_id):
        '''Annotate a document with a given tag or tags.

        Either a single tag can be given, or a list of tags by id.

        Args:
            doc_id (:class:`int`): id of the document to annotate.
            tag_id (:class:`int` or :class:`list[int]`): tag id or list of tag ids to annotate the document with.

        Returns:
            :class:`ObjectId` or :class:`int` or :class:`None`: id of the document if it was updated, or :class:`None`
        '''
        return self.document_manager.update_document_tags((doc_id, tag_id), ("$addToSet", "$addToSet"))

    def remove_tag(self, doc_id, tag_id):
        '''Remove tag annotations from a document.

        Either a single tag can be given, or a list of tags by id.

        Args:
            doc_id (:class:`int`): id of the document to remove annotation from.
            tag_id (:class:`int` or :class:`list[int]`): tag id or list of tag ids to remove from the document.

        Returns:
            :class:`ObjectId` or :class:`int` or :class:`None`: id of the document if it was updated, or None
        '''
        return self.document_manager.update_document_tags((doc_id, tag_id), ("$pullAll", "$pull"))

    def insert_tag(self, tag_name, inheritable=False):
        '''Create a new annotation tag with the given name.

        Tag Ids are auto-generated from the blackboard with the next available id.
        To create a control tag, use either 'FOR>' or 'POST>' as a prefix.

        Args:
            tag_name (:class:`str`): name of the tag to create.
            inheritable (:class:`bool`, optional): whether the tag should be inheritable or not.

        Returns:
            :class:`int`: id of the newly created annotation tag.

        Raises:
           :class:`ValueError`: If **tag_name** already exists in the blackboard.
        '''
        if self.tag_manager.tag_exists(tag_name):
            raise ValueError('Tag already exists')
        return self.tag_manager.insert_tag(tag_name, inheritable)

    def update_tag(self, tag_id, tag_name, inheritable=None):
        '''Update an annotation tag by id.

        The tag with id **tag_id** will be updated, for example, to be renamed with the new **tag_name**.

        Args:
            tag_id (:class:`int`): id of the tag to update.
            tag_name (:class:`str`): new name for the annotation tag.
            inheritable (:class:`bool`, optional): whether the tag should be inheritable or not.

        Returns:
            :class:`int`: id of the updated annotation tag.

        Raises:
            :class:`ValueError`: If **tag_name** already exists for a different id in the blackboard.
        '''
        tag = self.get_tag(tag_name)
        if tag is not None and tag[self.tag_manager.tag_id] != tag_id:
            raise ValueError('A tag with name "{name}" already exists with id: {id}'.format(name=tag_name, id=tag[self.tag_manager.tag_id]))
        return self.tag_manager.update_tag(tag_id, tag_name, inheritable)

    @check_admin('Admin rights required to delete tags.')
    def delete_tag(self, tag_id):
        '''Delete an annotation tag by id.

        .. danger:: Deleting a tag will also remove it from all documents in the blackboard!

        Args:
            tag_id (:class:`int`): id of the tag to delete.

        Returns:
            ???

        Raises:
            :class:`PermissionError`: If the user does not have admin privileges.
        '''
        return self.tag_manager.delete_tag(tag_id)

    def get_tag(self, tag):
        '''Retrieve an annotation tag by id or name.

        If an :class:`int` is passed as argument, it is assumed to be the tag id.
        If a :class:`str` is passed as argument, it is assumed to be the tag name.

        Args:
            tag (:class:`int` or :class:`str`): the id or name of the tag to retrieve.

        Returns:
            :class:`dict`: dictionary object representing the specified tag.
        '''
        return self.tag_manager.check_tag_type(tag, self.tag_manager.get_tag)

    def is_control_tag(self, tag):
        '''Check whether a tag is a control tag.

        If an :class:`int` is passed as argument, it is assumed to be the tag id.
        If a :class:`str` is passed as argument, it is assumed to be the tag name.

        Args:
            tag (:class:`int` or :class:`str`): the id or name of the tag to retrieve.

        Returns:
            :class:`bool`: True if control tag, False otherwise.
        '''
        return self.tag_manager.check_tag_type(tag, self.tag_manager.is_control_tag)

    def is_inheritable_tag(self, tag):
        '''Check whether a tag is inheritable.

        If an :class:`int` is passed as argument, it is assumed to be the tag id.
        If a :class:`str` is passed as argument, it is assumed to be the tag name.        

        Args:
            tag (:class:`int` or :class:`str`): the id or name of the tag to retrieve.

        Returns:
            :class:`bool`: True if tag is inheritable, False otherwise.
        '''
        return self.tag_manager.check_tag_type(tag, self.tag_manager.is_inheritable_tag)


class DateBasedBlackboard(Blackboard):
    '''DateBasedBlackboard object that acts as an interface for retrieving and inserting data from a date-based blackboard.

    Example:
        >>> blackboard = api.load_blackboard('ARTICLE')
        >>> blackboard.count() # count total documents
        >>> for doc in blackboard.find():
        >>> ... print("{date} - {title}".format(date=blackboard.get_date(doc), title=doc['T'])
    '''

    def __init__(self, settings):
        '''Constructor for DateBasedBlackboard objects.

        This should not be called directly. Blackboards can be accessed by loading them using the BlackboardAPI.

        Example:
            >>> api = BlackboardAPI(settings)
            >>> blackboard = api.load_blackboard('ARTICLE')
        '''
        super().__init__((settings[0], settings[1].upper(), settings[2]))
        self.document_manager = DateBasedDocumentManager(self)

    def get_date(self, doc):
        '''Get the date for a given document.

        Args:
            doc (:class:`dict`): the document to get the date for.

        Returns:
            :class:`datetime.datetime`: the datetime stored in the document's :class:`ObjectId`.

        Raises:
            :class:`ValueError`: If the "_id" field does not contain an :class:`ObjectId`.
        '''
        return self.document_manager.get_date(doc)

    def get_earliest_date(self):
        '''Retrieve the oldest document date in the blackboard.

        Returns:
            :class:`datetime.datetime`: the oldest datetime stored in the blackboard.
        '''
        return self.document_manager.get_earliest_date()

    def get_latest_date(self):
        '''Retrieve the most recent document date in the blackboard.

        Returns:
            :class:`datetime.datetime`: the most recent datetime stored in the blackboard.
        '''
        return self.document_manager.get_latest_date()
