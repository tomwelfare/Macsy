import sys
import os.path
import random
import unittest
import mongomock 
import pymongo
import itertools
home = '/'.join(os.path.abspath(__file__).split('/')[0:-2])
sys.path.insert(0, home)
import mock_data_generator
from datetime import datetime
from dateutil import parser as dtparser
from bson.objectid import ObjectId
from macsy.blackboards import blackboard_api
from macsy.blackboards.managers import tag_manager, document_manager, counter_manager

BlackboardAPI = blackboard_api.BlackboardAPI
TagManager = tag_manager.TagManager
DocumentManager = document_manager.DocumentManager
CounterManager = counter_manager.CounterManager

class TestBlackboards(unittest.TestCase):

    def setUp(self):
        self.api = BlackboardAPI(mock_data_generator.settings(), MongoClient=mock_data_generator.mock_client)
        self.bb = self.api.load_blackboard('FEED')

    def tearDown(self):
        del self.api
        del self.bb

    def test_bb_count(self):
        pass

    def test_bb_find(self):
        pass

    def test_insert(self):
        pass

    def test_update(self):
        pass

    def test_delete(self):
        pass

    def test_add_tag(self):
        pass

    def test_remove_tag(self):
        pass

    def test_insert_tag(self):
        pass

    def test_update_tag(self):
        pass

    def test_delete_tag(self):
        pass

    def test_bb_get_tag(self):
        pass

    def test_bb_is_control_tag(self):
        pass

    def test_bb_is_inheritable_tag(self):
        pass


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestBlackboards)
    unittest.TextTestRunner().run(suite)
