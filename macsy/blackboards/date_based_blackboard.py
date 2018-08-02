from macsy.blackboards import blackboard
from macsy.blackboards.managers import date_based_document_manager
Blackboard = blackboard.Blackboard
DBDocumentManager = date_based_document_manager.DateBasedDocumentManager

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
        self.document_manager = DBDocumentManager(self)

    def count(self, **kwargs):
        '''Count the number of documents in the blackboard.

        Args:
            tags (list[int], optional): filter documents to those that have all of the specified tags.
            without_tags (list[int], optional): filter documents to those that do not have have any of specified tags.
            fields (list[str], optional): filter documents to those that have the specified fields.
            without_fields (list[str], optional): filter documents to those that do not have the specified fields.
            min_date (list[str], optional): filter documents to those that occur after the given date.
            max_date (list[str], optional): filter documents to those that occur before the given date.
            query (dict): raw mongo query, bypassing other arguments.

        Returns:
            int: number of documents in the blackboard.
        '''
        super(**kwargs)

    def find(self, **kwargs):
        '''Return a cursor for documents in the blackboard.

        Args:
            tags (list[int], optional): filter documents to those that have all of the specified tags.
            without_tags (list[int], optional): filter documents to those that do not have have any of specified tags.
            fields (list[str], optional): filter documents to those that have the specified fields.
            without_fields (list[str], optional): filter documents to those that do not have the specified fields.
            min_date (list[str], optional): filter documents to those that occur after the given date.
            max_date (list[str], optional): filter documents to those that occur before the given date.
            query (dict): raw mongo query, bypassing other arguments.

        Returns:
            BlackboardCursor: cursor of results from the database.
        '''
        super(**kwargs)

    def get_date(self, doc):
        return self.document_manager.get_date(doc)

    def get_earliest_date(self):
        return self.document_manager.get_earliest_date()

    def get_latest_date(self):
        return self.document_manager.get_latest_date()
