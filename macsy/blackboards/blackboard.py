from macsy.blackboards import blackboard_cursor
from macsy.blackboards.managers import tag_manager, document_manager, counter_manager
BlackboardCursor = blackboard_cursor.BlackboardCursor
TagManager = tag_manager.TagManager
DocumentManager = document_manager.DocumentManager
CounterManager = counter_manager.CounterManager

def check_admin(error):
    def dec(func):
        def wrap(*args, **kwargs):
            if not args[0].admin_mode:
                raise PermissionError(error)
            return func(*args, **kwargs)
        return wrap
    return dec

class Blackboard():

    def __init__(self, settings):
        self._db, self._name, self.admin_mode = settings
        self._document_manager = DocumentManager(self)
        self._tag_manager = TagManager(self)
        self._counter_manager = CounterManager(self)

    def count(self, **kwargs):
        return self._document_manager.count(**kwargs)

    def find(self, **kwargs):
        return BlackboardCursor(self._document_manager.find(**kwargs))

    # Later on, need to add bulk insert/update/delete methods
    def insert(self, doc):
        return self._document_manager.insert(doc)

    def update(self, doc_id, updated_fields):
        return self._document_manager.update(doc_id, updated_fields)

    @check_admin('Admin rights required to delete documents.')
    def delete(self, doc_id):
        return self._document_manager.delete(doc_id)

    def insert_tag(self, tag_name, control=False, inheritable=False):
        return self._tag_manager.insert_tag(tag_name, control, inheritable)

    def update_tag(self, tag_id, tag_name, control=None, inheritable=None):
        return self._tag_manager.update_tag(tag_id, tag_name, control, inheritable)

    @check_admin('Admin rights required to delete tags.')
    def delete_tag(self, tag_id):
        return self._tag_manager.delete_tag(tag_id)

    def add_tag(self, doc_id, tag_id):
        return self._document_manager.add_tag(doc_id, tag_id)

    def remove_tag(self, doc_id, tag_id):
        return self._document_manager.remove_tag(doc_id, tag_id)

    def get_tag(self, tag_id=None, tag_name=None):
        return self._tag_manager.get_tag(tag_id, tag_name)

    def is_control_tag(self, tag_id=None, tag_name=None):
        return self._tag_manager.is_control_tag(tag_id, tag_name)

    def is_inheritable_tag(self, tag_id=None, tag_name=None):
        return self._tag_manager.is_inheritable_tag(tag_id, tag_name)
