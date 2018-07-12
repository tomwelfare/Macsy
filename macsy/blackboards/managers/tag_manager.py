import pymongo
from bson.objectid import ObjectId
from macsy.blackboards.managers import base_manager, counter_manager
CounterManager = counter_manager.CounterManager

class TagManager(base_manager.BaseManager):

    tag_suffix = '_TAGS'
    tag_id = '_id'
    tag_name = 'Nm'
    tag_control = 'Ctrl'
    tag_inherit = 'DInh'
    tag_control_for = 'FOR>'
    tag_control_post = 'POST>'

    def __init__(self, parent):
        super().__init__(parent)
        self._tag_collection = self._parent._db[self._parent._name + TagManager.tag_suffix]
        
    def insert_tag(self, tag_name, control = False, inheritable = False):
        if self._tag_exists(tag_name):
            raise ValueError('Tag already exists')
        ctrl = 0 if control is not True else 1
        if TagManager.tag_control_for or TagManager.tag_control_post in tag_name:
            ctrl = 1
        inherit = 0 if inheritable is not True else 1
        tag = {TagManager.tag_id : self._parent._counter_manager.next_tag_id_and_increment(), 
            TagManager.tag_name : tag_name, 
            TagManager.tag_control : ctrl, 
            TagManager.tag_inherit : inherit}
        return self._tag_collection.insert(tag)

    def update_tag(self, tag_id, tag_name, control = None, inheritable = None):
        raise NotImplementedError()

    def delete_tag(self, tag_id):
        self._remove_tag_from_all(tag_id)
        return self._tag_collection.remove({TagManager.tag_id : tag_id})

    def get_tag(self, tag_id = None, tag_name = None):
        if tag_id is not None:
            return self._tag_collection.find_one({TagManager.tag_id : tag_id})
        if tag_name is not None:
            return self._tag_collection.find_one({TagManager.tag_name : tag_name})

    def is_control_tag(self, tag_id = None, tag_name = None):
        return self._tag_has_property(TagManager.tag_control, tag_id, tag_name)

    def is_inheritable_tag(self, tag_id = None, tag_name = None):
        return self._tag_has_property(TagManager.tag_inherit, tag_id, tag_name)

    def _tag_has_property(self, tag_property, tag_id = None, tag_name = None):
        tag = self.get_tag(tag_id = tag_id) if tag_id is not None else self.get_tag(tag_name = tag_name)
        test = tag[tag_property] if (tag is not None and tag_property in tag) else False
        return bool(test)

    def get_canonical_tag(self, tag):
        full_tag = self.get_tag(tag_name=tag) if type(tag) is str else self.get_tag(tag_id=tag)
        if full_tag is None:
            raise ValueError('Tag does not exist: {}'.format(tag))
        return full_tag

    def _tag_exists(self, tag_name):
        raise NotImplementedError()

    def _remove_tag_from_all(self, tag_id):
        print('Removing tag {} from {} documents.'.format(tag_id, self._parent.count(tags=[tag_id])))
        for doc in self._parent.find(tags=[tag_id]):
            self._parent.remove_tag(doc[self._parent.doc_id], tag_id)