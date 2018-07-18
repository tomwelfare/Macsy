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
from macsy.blackboards import blackboard_api, date_based_blackboard
from macsy.blackboards.managers import tag_manager, date_based_document_manager, counter_manager

BlackboardAPI = blackboard_api.BlackboardAPI
DateBasedBlackboard = date_based_blackboard.DateBasedBlackboard
TagManager = tag_manager.TagManager
DateBasedDocumentManager = date_based_document_manager.DateBasedDocumentManager
CounterManager = counter_manager.CounterManager

class TestDateBasedBlackboards(unittest.TestCase):

    def setUp(self):
        self.api = BlackboardAPI(mock_data_generator.settings(), MongoClient=mock_data_generator.mock_client)
        self.bb = self.api.load_blackboard('ARTICLE')

    def tearDown(self):
        del self.api
        del self.bb

    def test_bb_count(self):
        self.assertEqual(self.bb.count(), 10)
        self.assertEqual(self.bb.count(query={'T' : 'Title 3'}), 1)
        self.assertEqual(self.bb.count(max_date=['02-01-2016'], tags = ['FOR>Tag_11', 12]), 8)
        self.assertEqual(self.bb.count(query={'BLANK' : 'Title 3'}), 0)

    def test_bb_find(self):
        self.assertEqual(len(self.bb.find()), 10)
        self.assertEqual(len([x for x in self.bb.find(tags = [3])]), 2)
        self.assertEqual(len(self.bb.find(tags = ['FOR>Tag_11', 12])), 10)
        self.assertEqual(len(self.bb.find(min_date=['01-01-2016'], tags = ['FOR>Tag_11', 12])), 3)
        self.assertEqual(len([x for x in self.bb.find(max_date=['02-01-2016'], tags = ['FOR>Tag_11', 12])]), 8)
        self.assertEqual(len(self.bb.find(tags = ['FOR>Tag_11', 5])), 2)
        self.assertEqual([x for x in self.bb.find(query={'T' : 'Title 3'})][0]['T'], 'Title 3')

        with self.assertRaises(ValueError): self.bb.find(tags = [1, 13])
        with self.assertRaises(ValueError): self.bb.find(tags = ['Tag_4', 13])
        with self.assertRaises(ValueError): self.bb.find(tags = ['Tag_4', -5])

    def test_insert(self):
        # Generate a doc, check # of docs, insert it, check it's incremented
        obj_id = ObjectId.from_datetime(dtparser.parse('21-10-2017'))
        expected = 11
        self.assertEqual(self.bb.count(), expected-1)
        self.assertEqual(self.bb.insert({DateBasedDocumentManager.doc_id : obj_id, 'Overwritten' : False, 'Inserted' : True, 'Tg' : [1, 2, 3]}), obj_id)
        self.assertEqual(self.bb.count(), expected)
        self.assertEqual([x for x in self.bb.find(query={'Inserted' : True})][0]['_id'], obj_id)
        
        # Try to insert it again
        self.assertEqual(self.bb.insert({DateBasedDocumentManager.doc_id : obj_id, 'Overwritten' : True, 'Updated' : True, 'Tg' : [4, 5]}), obj_id)
        self.assertEqual(self.bb.count(), expected)
        self.assertEqual([x for x in self.bb.find(query={'Updated' : True})][0]['_id'], obj_id)
        self.assertEqual(self.bb.count(query={'Overwritten' : False}), 0)
        self.assertEqual([x for x in self.bb.find(query={'Overwritten' : True})][0]['_id'], obj_id)
        self.assertEqual([x for x in self.bb.find(tags=[1, 2, 3, 4, 5])][0]['_id'], obj_id)

        # Insert a document without an id and generate one
        self.assertEqual(self.bb.insert({'Blank_id' : True}).generation_time.date(), datetime.now().date())

    def test_update(self):
        obj_id = self.bb.insert({'Overwritten' : False, 'Inserted' : True, 'Tg' : [1, 2, 3]})
        self.assertEqual(self.bb.update(obj_id, {'Overwritten' : True, 'Inserted' : False, 'Fds' : [104], 'Tg' : [1, 4, 6]}), obj_id)
        # Fails due to bug if older version of mockmongo is used
        self.assertEqual([x for x in self.bb.find(query={'Overwritten' : True, 'Tg' : [1, 2, 3, 4, 6]})][0]['_id'], obj_id)
        self.assertEqual(self.bb.update(obj_id, {'Fds' : 111, 'Tg' : 5}), obj_id)
        self.assertEqual([x for x in self.bb.find(query={'Overwritten' : True, 'Fds' : [104, 111], 'Tg' : [1, 2, 3, 4, 6, 5]})][0]['_id'], obj_id)

    def test_delete(self):
        obj_id = self.bb.insert({'Deleted' : False})
        expected = 11
        with self.assertRaises(PermissionError): self.bb.delete(obj_id)
        self.assertEqual(self.bb.count(), expected)

        # Add test as admin
        self.api = BlackboardAPI(mock_data_generator.admin_settings(), MongoClient=mock_data_generator.mock_client)
        self.bb = self.api.load_blackboard('ARTICLE')
        obj_id = self.bb.insert({'Deleted' : False})
        expected = 11
        self.assertEqual(self.bb.count(), expected)
        self.assertEqual(self.bb.count(query={'Deleted' : False}), 1)
        self.bb.delete(obj_id)
        self.assertEqual(self.bb.count(query={'Deleted' : False}), 0)
        self.assertEqual(self.bb.count(), expected-1)

    def test_add_tag(self):
        obj_id = self.bb.insert({'hasTags' : False})
        self.bb.add_tag(obj_id, 1)
        self.assertEqual(self.bb.count(tags=[1]), 3)
        self.bb.add_tag(obj_id, [2])
        self.assertEqual(self.bb.count(tags=[1, 2]), 2)
        self.bb.add_tag(obj_id, [3, 4])
        self.assertEqual([x for x in self.bb.find(tags=[1, 2, 3])][0][DateBasedDocumentManager.doc_id], obj_id)
        self.bb.add_tag(obj_id, 4)
        self.assertEqual([x for x in self.bb.find(query={'Tg' : [1, 2, 3, 4]})][0][DateBasedDocumentManager.doc_id], obj_id)
        self.bb.add_tag(obj_id, [6,7])
        self.assertEqual([x for x in self.bb.find(query={'Tg' : [1, 2, 3, 4, 6, 7]})][0][DateBasedDocumentManager.doc_id], obj_id)        

    def test_remove_tag(self):
        obj_id = self.bb.insert({'hasTags' : True, 'Tg' : [1, 2, 3, 4, 5]})
        result = self.bb.remove_tag(obj_id, 3)
        self.assertEqual(result['err'], None)
        self.assertEqual([x for x in self.bb.find(tags=[1, 2, 4, 5])][0][DateBasedDocumentManager.doc_id], obj_id)
        result = self.bb.remove_tag(obj_id, [1,5])
        self.assertEqual(result['err'], None)
        self.assertEqual([x for x in self.bb.find(query={'Tg' : [2, 4]})][0][DateBasedDocumentManager.doc_id], obj_id)
        result = self.bb.remove_tag(obj_id, [2,8])
        self.assertEqual(result['err'], None)
        self.assertEqual([x for x in self.bb.find(query={'Tg' : [4]})][0][DateBasedDocumentManager.doc_id], obj_id)

    def test_insert_tag(self):
        self.assertEqual(self.bb.insert_tag('Tag_Not_Control'), 13)
        self.assertEqual(self.bb.get_tag('Tag_Not_Control')[TagManager.tag_id], 13)
        self.assertEqual(self.bb.get_tag('Tag_Not_Control')[TagManager.tag_control], 0)
        self.assertEqual(self.bb.is_control_tag(13), False)

        self.assertEqual(self.bb.insert_tag('FOR>Tag_14'), 14)
        self.assertEqual(self.bb.get_tag('FOR>Tag_14')[TagManager.tag_id], 14)
        self.assertEqual(self.bb.get_tag('FOR>Tag_14')[TagManager.tag_control], 1)
        self.assertEqual(self.bb.is_control_tag(14), True)
        self.assertEqual(self.bb.is_inheritable_tag(14), False)

        with self.assertRaises(ValueError): self.bb.insert_tag('Tag_1')
        with self.assertRaises(ValueError): self.bb.insert_tag('FOR>Tag_14')
        with self.assertRaises(ValueError): self.bb.insert_tag('POST>Tag_12')

    def test_update_tag(self):
        self.bb.update_tag(1, "Tag_1_Renamed")
        self.assertEqual(self.bb.get_tag(1)['Nm'], "Tag_1_Renamed")
        self.bb.update_tag(1, "Tag_1_Renamed", True)
        self.assertEqual(self.bb.get_tag(1)['DInh'], 1)
        self.bb.update_tag(1, "FOR>Tag_1_Renamed", True)
        self.assertEqual(self.bb.get_tag(1)['Ctrl'], 1)

    def test_delete_tag(self):
        with self.assertRaises(PermissionError): self.bb.delete_tag(1)
        self.assertEqual(self.bb.get_tag(1)['Nm'], 'Tag_1')

        # Add test as admin
        self.api = BlackboardAPI(mock_data_generator.admin_settings(), MongoClient=mock_data_generator.mock_client)
        self.bb = self.api.load_blackboard('ARTICLE')
        obj_id = self.bb.insert_tag('To_be_deleted')
        expected = 11
        self.assertEqual(self.bb.get_tag('To_be_deleted')['Nm'], 'To_be_deleted')
        self.assertEqual(self.bb.delete_tag(obj_id)['n'], 1)
        self.assertEqual(self.bb.get_tag('To_be_deleted'), None)

        self.assertEqual(self.bb.count(tags=[2]), 2)
        self.assertEqual(self.bb.delete_tag(2)['n'], 1)
        with self.assertRaises(ValueError): self.bb.count(tags=[2])

    def test_bb_get_tag(self):
        # Bad input
        self.assertEqual(self.bb.get_tag(55), None)
        self.assertEqual(self.bb.get_tag('3'), None)
        self.assertEqual(self.bb.get_tag(3333), None)

        # Retrieval
        self.assertEqual(self.bb.get_tag(1), {'Ctrl': 0, '_id': 1, 'Nm': 'Tag_1'})
        self.assertEqual(self.bb.get_tag(2), {'Ctrl': 0, '_id': 2, 'Nm': 'Tag_2'})
        self.assertEqual(self.bb.get_tag('Tag_3'), {'Ctrl': 0, '_id': 3, 'Nm': 'Tag_3'})
        self.assertEqual(self.bb.get_tag(12), {'Ctrl': 1, 'Nm': 'POST>Tag_12', '_id': 12})
        

    def test_bb_tag_has_property(self):
        # Bad input for control
        self.assertEqual(self.bb.is_control_tag(55), False)
        self.assertEqual(self.bb.is_control_tag('3'), False)
        
        # Bad input for inheritance
        self.assertEqual(self.bb.is_inheritable_tag(55), False)
        self.assertEqual(self.bb.is_inheritable_tag('3'), False)

        # Checking for control
        self.assertEqual(self.bb.is_control_tag(11), True)
        self.assertEqual(self.bb.is_control_tag('FOR>Tag_11'), True)
        self.assertEqual(self.bb.is_control_tag(4), False)
        self.assertEqual(self.bb.is_control_tag('Tag_4'), False)

        # Checking for inheritance (should add True test)
        self.assertEqual(self.bb.is_inheritable_tag(11), False)
        self.assertEqual(self.bb.is_inheritable_tag(4), False)
        self.assertEqual(self.bb.is_inheritable_tag(4), False)
        self.assertEqual(self.bb.is_inheritable_tag('Tag_4'), False)
        
    def test_bb_get_date(self):
        self.assertEqual(str(self.bb.get_date({DateBasedDocumentManager.doc_id: ObjectId.from_datetime(dtparser.parse('21-10-2017'))})), '2017-10-21 00:00:00+00:00')
        with self.assertRaises(ValueError): self.bb.get_date({'different_id': ObjectId.from_datetime(dtparser.parse('21-10-2017'))})
        with self.assertRaises(ValueError): self.bb.get_date({DateBasedDocumentManager.doc_id: 1})

    def test_get_extremal_date(self):
        self.assertEqual(str(self.bb.get_earliest_date()), '2009-01-01 00:00:00+00:00')
        self.assertEqual(str(self.bb.get_latest_date()), '2018-01-01 00:00:00+00:00')      

    


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestDateBasedBlackboards)
    unittest.TextTestRunner().run(suite)
