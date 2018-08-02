from functools import wraps, partial
from macsy.blackboards import blackboard_cursor
from macsy.blackboards.managers import tag_manager, document_manager, counter_manager
BlackboardCursor = blackboard_cursor.BlackboardCursor
TagManager = tag_manager.TagManager
DocumentManager = document_manager.DocumentManager
CounterManager = counter_manager.CounterManager

def check_admin(error):
    def dec(func):
        @wraps(func)
        def wrap(*args, **kwargs):
            if not args[0].admin_mode:
                raise PermissionError(error)
            return func(*args, **kwargs)
        return wrap
    return dec

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
            tags (list[int], optional): filter documents to those that have all of the specified tags.
            without_tags (list[int], optional): filter documents to those that do not have have any of specified tags.
            fields (list[str], optional): filter documents to those that have the specified fields.
            without_fields (list[str], optional): filter documents to those that do not have the specified fields.
            query (dict): raw mongo query, bypassing other arguments

        Returns:
            int: number of documents in blackboard.
        '''
        return self.document_manager.count(**kwargs)

    def find(self, **kwargs):
        '''Return a cursor for documents in the blackboard.

        Args:
            tags (list[int], optional): filter documents to those that have all of the specified tags.
            without_tags (list[int], optional): filter documents to those that do not have have any of specified tags.
            fields (list[str], optional): filter documents to those that have the specified fields.
            without_fields (list[str], optional): filter documents to those that do not have the specified fields.
            query (dict): raw mongo query, bypassing other arguments

        Returns:
            BlackboardCursor: cursor of results from the database.
        '''
        return BlackboardCursor(self.document_manager.find(**kwargs))

    
    def insert(self, doc):
        '''Insert a new document into the blackboard.

        Documents which do not contain an id field ('_id') will have a new id auto-generated for them.
        If the id for the document already exists, the document is updated with the new details (upsert).

        Args:
            doc (dict): dictionary containing the fields and values to insert into the blackboard.

        Returns:
            int: id of the inserted document in the blackboard.
        '''
        return self.document_manager.insert(doc)

    def update(self, doc_id, updated_fields):
        '''Update an existing document in the blackboard.

        Documents which do not contain an id field ('_id') will have a new id auto-generated for them.
        If the id for the document already exists, the document is updated with the new details (upsert).

        Args:
            doc_id (int): id of the document to update.
            updated_fields (dict): dictionary containing the fields to be updated, along with their new values.

        Returns:
            int or None: id of the updated document in the blackboard, or None if id does not exist.
        '''
        return self.document_manager.update(doc_id, updated_fields)

    @check_admin('Admin rights required to delete documents.')
    def delete(self, doc_id):
        '''Delete a document from the blackboard.

        Documents can only be deleted by admin users.

        Args:
            doc_id (int): id of the document to delete.

        Returns:
            ???

        Raises:
            PermissionError: If the user does not have admin privileges.
        '''
        return self.document_manager.delete(doc_id)

    def get_all_tags(self):
        '''Get a list of all the tags in the blackboard.

        Returns:
            list[dict]: list of tags, where each tag is a dictionary of the key-values from the blackboard.
        '''
        return self.tag_manager.get_all_tags()

    def add_tag(self, doc_id, tag_id):
        '''Annotate a document with a given tag or tags.

        Either a single tag can be given, or a list of tags by id.

        Args:
            doc_id (int): id of the document to annotate.
            tag_id (int or list[int]): tag id or list of tag ids to annotate the document with.

        Returns:
            int or None: id of the document if it was updated, or None
        '''
        return self.document_manager.update_document_tags((doc_id, tag_id), ("$addToSet", "$addToSet"))

    def remove_tag(self, doc_id, tag_id):
        return self.document_manager.update_document_tags((doc_id, tag_id), ("$pullAll", "$pull"))

    def insert_tag(self, tag_name, inheritable=False):
        if self.tag_manager.tag_exists(tag_name):
            raise ValueError('Tag already exists')
        return self.tag_manager.insert_tag(tag_name, inheritable)

    def update_tag(self, tag_id, tag_name, inheritable=None):
        return self.tag_manager.update_tag(tag_id, tag_name, inheritable)

    @check_admin('Admin rights required to delete tags.')
    def delete_tag(self, tag_id):
        return self.tag_manager.delete_tag(tag_id)

    def get_tag(self, tag):
        return self.tag_manager.check_tag_type(tag, self.tag_manager.get_tag)

    def is_control_tag(self, tag):
        return self.tag_manager.check_tag_type(tag, self.tag_manager.is_control_tag)

    def is_inheritable_tag(self, tag):
        return self.tag_manager.check_tag_type(tag, self.tag_manager.is_inheritable_tag)

