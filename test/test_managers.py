import sys
import os.path
import random
import unittest
import mongomock 
import pymongo
import itertools
home = '/'.join(os.path.abspath(__file__).split('/')[0:-2])
sys.path.insert(0, home)
from datetime import datetime
from dateutil import parser as dtparser
from test import mock_data_generator
from bson.objectid import ObjectId
from macsy.blackboards import blackboard_api
from macsy.blackboards.managers import tag_manager, document_manager, counter_manager, date_based_document_manager

BlackboardAPI = blackboard_api.BlackboardAPI
TagManager = tag_manager.TagManager
DocumentManager = document_manager.DocumentManager
DateBasedDocumentManager = date_based_document_manager.DateBasedDocumentManager
CounterManager = counter_manager.CounterManager

class TestManagers(unittest.TestCase):

    def setUp(self):
        random.seed(1234)
        # Settings dont need to be changed when mocking
        self.settings = {'user' : 'test_user', 'password' : 'password', 'dbname' : 'testdb', 
            'dburl' : 'mongodb://localhost:27017'}
        self.api = BlackboardAPI(self.settings, MongoClient=mock_data_generator.mock_client)
        self.bb = self.api.load_blackboard('ARTICLE')

    def tearDown(self):
        del self.api
        del self.bb

    def test_init_tag_manager(self):
        with self.assertRaises(UserWarning): TagManager(self.bb)
        with self.assertRaises(UserWarning): TagManager(None)

    def test_init_document_manager(self):
        with self.assertRaises(UserWarning): DocumentManager(self.bb)
        with self.assertRaises(UserWarning): DocumentManager(None)

    def test_init_date_based_document_manager(self):
        with self.assertRaises(UserWarning): DateBasedDocumentManager(self.bb)
        with self.assertRaises(UserWarning): DateBasedDocumentManager(None)

    def test_init_counter_manager(self):
        with self.assertRaises(UserWarning): CounterManager(self.bb)
        with self.assertRaises(UserWarning): CounterManager(None)


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestManagers)
    unittest.TextTestRunner().run(suite)
