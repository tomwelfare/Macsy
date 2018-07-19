import pymongo
from bson.objectid import ObjectId
from macsy.blackboards.managers import base_manager

class TagManager(base_manager.BaseManager):

    tag_suffix = '_TAGS'
    tag_id = '_id'
    tag_name = 'Nm'
    tag_control = 'Ctrl'
    tag_inherit = 'DInh'
    control_tags = ['FOR>', 'POST>']

    def __init__(self, parent):
        suffix = TagManager.tag_suffix
        super().__init__(parent, suffix)

    def insert_tag(self, tag_name, inheritable=False):
        ctrl = 1 if any(map(tag_name.startswith, TagManager.control_tags)) else 0
        inherit = 0 if inheritable is not True else 1
        tag = {TagManager.tag_id : self._parent._counter_manager.get_next_id_and_increment(self._parent._counter_manager.counter_tag), 
            TagManager.tag_name : tag_name, 
            TagManager.tag_control : ctrl, 
            TagManager.tag_inherit : inherit}
        return self._collection.insert(tag)

    def update_tag(self, tag_id, tag_name, inheritable=None):
        tag = self.get_tag(tag_id)
        if inheritable:
            tag[TagManager.tag_inherit] = 1
        tag[TagManager.tag_name] = tag_name
        if any(map(tag_name.startswith, TagManager.control_tags)):
            tag[TagManager.tag_control] = 1
        return self._collection.update({TagManager.tag_id : tag_id}, {"$set" : tag})

    def delete_tag(self, tag_id):
        self._remove_tag_from_all(tag_id)
        return self._collection.remove({TagManager.tag_id : tag_id})

    def get_tag(self, tag_id=None, tag_name=None):
        if tag_id is not None:
            return self._collection.find_one({TagManager.tag_id : tag_id})
        else:
            return self._collection.find_one({TagManager.tag_name : tag_name})

    def is_control_tag(self, tag_id=None, tag_name=None):
        return self._tag_has_property(TagManager.tag_control, tag_id, tag_name)

    def is_inheritable_tag(self, tag_id=None, tag_name=None):
        return self._tag_has_property(TagManager.tag_inherit, tag_id, tag_name)

    def check_tag_type(self, tag, func):
        return func(tag_name=tag) if type(tag) is str else func(tag_id=tag)

    def tag_exists(self, tag_name):
        exists = self._collection.find_one({TagManager.tag_name : tag_name})
        return True if exists is not None else False

    def _tag_has_property(self, tag_property, tag_id=None, tag_name=None):
        tag = self.get_tag(tag_id=tag_id) if tag_id is not None else self.get_tag(tag_name=tag_name)
        test = tag[tag_property] if (tag is not None and tag_property in tag) else False
        return bool(test)

    def get_canonical_tag(self, tag):
        full_tag = self.get_tag(tag_name=tag) if type(tag) is str else self.get_tag(tag_id=tag)
        if full_tag is None:
            raise ValueError('Tag does not exist: {}'.format(tag))
        return full_tag

    def _remove_tag_from_all(self, tag_id):
        for doc in self._parent.find(tags=[tag_id]):
            self._parent.remove_tag(doc[self._parent._document_manager.doc_id], tag_id)