import pymongo
from macsy.blackboards.managers import base_manager

class CounterManager(base_manager.BaseManager):

    counter_suffix = '_COUNTER'
    counter_id = '_id'
    counter_next = 'NEXT_ID'
    counter_type = 'BLACKBOARD_TYPE'
    counter_type_standard = 'STANDARD'
    counter_type_date_based = 'DATE_BASED'
    counter_tag = "tag_counter"

    def __init__(self, parent):
        super().__init__(parent)
        self._counter_collection =  self._parent._db[self._parent._name + CounterManager.counter_suffix]

    def get_next_tag_id_and_increment(self):
        tag_id = self.get_next_tag_id()
        self._increment_next_tag_id(tag_id)
        return tag_id

    def get_next_tag_id(self):
        result = self._counter_collection.find_one({CounterManager.counter_id :CounterManager.counter_next})
        return result[CounterManager.counter_tag]

    def _increment_next_tag_id(self, tag_id):
        next_id = {CounterManager.counter_id : CounterManager.counter_next, "$set" : {CounterManager.counter_tag : int(tag_id+1)}}
        self._counter_collection.insert(next_id)
        