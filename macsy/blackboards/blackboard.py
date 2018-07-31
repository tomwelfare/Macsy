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
        self.document_manager = DocumentManager(self)
        self.tag_manager = TagManager(self)
        self.counter_manager = CounterManager(self)

    def count(self, **kwargs):
        return self.document_manager.count(**kwargs)

    def find(self, **kwargs):
        return BlackboardCursor(self.document_manager.find(**kwargs))

    # Later on, need to add bulk insert/update/delete methods
    def insert(self, doc):
        return self.document_manager.insert(doc)

    def update(self, doc_id, updated_fields):
        return self.document_manager.update(doc_id, updated_fields)

    @check_admin('Admin rights required to delete documents.')
    def delete(self, doc_id):
        return self.document_manager.delete(doc_id)

    def get_all_tags(self):
        return self.tag_manager.get_all_tags()

    def add_tag(self, doc_id, tag_id):
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
