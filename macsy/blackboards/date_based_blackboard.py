from macsy.blackboards import blackboard
from macsy.managers import date_based_document_manager
Blackboard = blackboard.Blackboard
DBDocumentManager = date_based_document_manager.DateBasedDocumentManager

class DateBasedBlackboard(Blackboard):

    def __init__(self, settings):
        super().__init__((settings[0], settings[1].upper(), settings[2]))
        self._document_manager = DBDocumentManager(self)

    def get_date(self, doc):
        return self._document_manager.get_date(doc)

    def get_earliest_date(self):
        return self._document_manager.get_earliest_date()

    def get_latest_date(self):
        return self._document_manager.get_latest_date()
