'''Blackboards are objects which provide an interface to data stored in the database.'''

from macsy.utils import check_admin
from macsy.cursors import BlackboardCursor
from macsy.managers import TagManager, DocumentManager, DateBasedDocumentManager, CounterManager

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
        self.counter_manager = CounterManager(self)
        self.document_manager = DocumentManager(self)
        self.tag_manager = TagManager(self)

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
