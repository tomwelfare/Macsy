import sys
import os.path
import random
import unittest
import mongomock 
import pymongo
import itertools
home = '/'.join(os.path.abspath(__file__).split('/')[0:-2])
sys.path.insert(0, home)
from test import mock_data_generator
from datetime import datetime
from dateutil import parser as dtparser
from bson.objectid import ObjectId
from macsy.blackboards import blackboard_api, blackboard, date_based_blackboard
from macsy.blackboards.managers import counter_manager

CounterManager = counter_manager.CounterManager
BlackboardAPI = blackboard_api.BlackboardAPI
Blackboard = blackboard.Blackboard
DateBasedBlackboard = date_based_blackboard.DateBasedBlackboard

class TestBlackboardAPI(unittest.TestCase):

    def teatDown(self):
        del self.api

    def test_api_load_blackboard(self):
        self.api = BlackboardAPI(mock_data_generator.settings(), MongoClient=mock_data_generator.mock_client)

        # Correctly called returns correct type
        self.assertIsInstance(self.api.load_blackboard('ARTICLE'), DateBasedBlackboard)
        self.assertIsInstance(self.api.load_blackboard('ARTICLE', date_based=True), DateBasedBlackboard)
        self.assertIsInstance(self.api.load_blackboard('FEED'), Blackboard)
        self.assertIsInstance(self.api.load_blackboard('FEED', date_based=False), Blackboard)

        # Incorrectly called raises error
        with self.assertRaises(ValueError): self.api.load_blackboard('ARTICLE_TAGS')
        with self.assertRaises(ValueError): self.api.load_blackboard('article_counter')
        with self.assertRaises(ValueError): self.api.load_blackboard('FEED', date_based=True)
        with self.assertRaises(ValueError): self.api.load_blackboard('ARTICLE', date_based=False)
        with self.assertRaises(ValueError): self.api.load_blackboard('ARTICLE2', date_based=False)

    def test_api_drop_blackboard(self):
        # Non-admin user
        self.api = BlackboardAPI(mock_data_generator.settings(), MongoClient=mock_data_generator.mock_client)

        # Check they exist before attempting drop
        blackboards = [('ARTICLE', True), ('ARTICLE2', True), ('FEED', True), ('article', False)]
        for bb in blackboards:
            self.assertEqual(self.api.blackboard_exists(bb[0]), bb[1])

        with self.assertRaises(ValueError): self.api.drop_blackboard('ARTICLE_TAGS')
        with self.assertRaises(ValueError): self.api.drop_blackboard('article_counter')
        with self.assertRaises(PermissionError): self.api.drop_blackboard('ARTICLE')
        with self.assertRaises(PermissionError): self.api.drop_blackboard('article')

        # Check they still exist after failed drop
        for bb in blackboards:
            self.assertEqual(self.api.blackboard_exists(bb[0]), bb[1])

        # Test admin drop
        self.api = BlackboardAPI(mock_data_generator.admin_settings(), MongoClient=mock_data_generator.mock_client)
        
        self.api.drop_blackboard('ARTICLE')
        self.assertEqual(self.api.blackboard_exists(blackboards[0][0]), not blackboards[0][1])
        for bb in blackboards[1:]:
            self.assertEqual(self.api.blackboard_exists(bb[0]), bb[1])

        self.api.drop_blackboard('ARTICLE2')
        self.assertEqual(self.api.blackboard_exists('ARTICLE2'), False)

        self.api.drop_blackboard('FEED')
        self.assertEqual(self.api.blackboard_exists('FEED'), False)

    def test_api_get_blackboard_names(self):
        self.api = BlackboardAPI(mock_data_generator.settings(), MongoClient=mock_data_generator.mock_client)
        self.assertSetEqual(set(self.api.get_blackboard_names()), set(['FEED', 'ARTICLE', 'ARTICLE2']))

    def test_api_get_blackboard_type(self):
        self.api = BlackboardAPI(mock_data_generator.settings(), MongoClient=mock_data_generator.mock_client)
        
        # Test getting date-based
        self.assertEqual(self.api.get_blackboard_type('ARTICLE'), CounterManager.counter_type_date_based)
        self.assertEqual(self.api.get_blackboard_type('ARTICLE', date_based=True), CounterManager.counter_type_date_based)
        with self.assertRaises(ValueError): self.api.get_blackboard_type('ARTICLE', date_based=False)

        # Test getting standard
        self.assertEqual(self.api.get_blackboard_type('FEED'), CounterManager.counter_type_standard)
        self.assertEqual(self.api.get_blackboard_type('FEED', date_based=False), CounterManager.counter_type_standard)
        with self.assertRaises(ValueError): self.api.get_blackboard_type('FEED', date_based=True)
        
        # Test getting a different date-based
        self.assertEqual(self.api.get_blackboard_type('ARTICLE2'), CounterManager.counter_type_date_based)

        # Test getting a non-existing blackboard
        self.assertEqual(self.api.get_blackboard_type('MISSING'), None)
        self.assertEqual(self.api.get_blackboard_type('MISSING', date_based=True), CounterManager.counter_type_date_based)
        self.assertEqual(self.api.get_blackboard_type('MISSING', date_based=False), CounterManager.counter_type_standard)

    def test_setting_validation(self):
        settings = {'user' : 'dbadmin', 'dbname' : 'testdb', 'dburl' : 'mongodb://localhost:27017'}
        with self.assertRaises(ValueError): self.api = BlackboardAPI(settings, MongoClient=mock_data_generator.mock_client)
        settings = {'user' : 'dbadmin', 'dog' : 'dog', 'dbname' : 'testdb', 'dburl' : 'mongodb://localhost:27017'}
        with self.assertRaises(ValueError): self.api = BlackboardAPI(settings, MongoClient=mock_data_generator.mock_client)

if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestBlackboardAPI)
    unittest.TextTestRunner().run(suite)
