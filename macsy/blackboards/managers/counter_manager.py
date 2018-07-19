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
    counter_doc = "doc_counter"

    def __init__(self, parent):
        super().__init__(parent, CounterManager.counter_suffix)

    def get_next_id_and_increment(self, field):
        next_id = self.get_next_id(field)
        self._increment_next_id(next_id, field)
        return next_id

    def get_next_id(self, field):
        result = self._collection.find_one({CounterManager.counter_id : CounterManager.counter_next})
        return result[field]

    def _increment_next_id(self, current_id, field):
        next_id = {"$set" : {field : int(current_id+1)}}
        self._collection.update({CounterManager.counter_id : CounterManager.counter_next}, next_id)
        