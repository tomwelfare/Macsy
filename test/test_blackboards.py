import random
import unittest
import mongomock 
import pymongo
import itertools
from datetime import datetime
from dateutil import parser as dtparser
from bson.objectid import ObjectId
from macsy.blackboards import blackboard_api, blackboard, date_based_blackboard
from macsy.managers import tag_manager, document_manager, counter_manager


BlackboardAPI = blackboard_api.BlackboardAPI
Blackboard = blackboard.Blackboard
DateBasedBlackboard = date_based_blackboard.DateBasedBlackboard
TagManager = tag_manager.TagManager
DocumentManager = document_manager.DocumentManager
CounterManager = counter_manager.CounterManager

class TestBlackboards(unittest.TestCase):

    def mock_client(*args, **kwargs):
        client = mongomock.MongoClient(*args, **kwargs)
        db = client['testdb']

        TestBlackboards.generate_date_based_blackboard(db,'ARTICLE')
        TestBlackboards.generate_date_based_blackboard(db,'ARTICLE2')
        TestBlackboards.generate_standard_blackboard(db,'FEED')

        return client

    def generate_date_based_blackboard(db, blackboard_name):
        blackboard_name = blackboard_name.upper()

        # Generate tags collection
        tags_coll = db[blackboard_name + TagManager.tag_suffix]
        tag_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        tgs = [{'_id': x, 'Nm': 'Tag_{}'.format(x), 'Ctrl': 0} for x in tag_ids]
        tgs.extend([{'_id': 11, 'Nm': 'FOR>Tag_11', 'Ctrl': 1},{'_id': 12, 'Nm': 'POST>Tag_12', 'Ctrl': 1}])    
        tags_coll.insert(tgs)

        # Generate counter collection
        counter_coll = db[blackboard_name + CounterManager.counter_suffix]
        counter_coll.insert({"_id" : CounterManager.counter_type, CounterManager.counter_type : CounterManager.counter_type_date_based})
        counter_coll.insert({"_id" : CounterManager.counter_next, CounterManager.counter_tag : tags_coll.count() + 1})

        # Generate date_based collections
        document_colls = {year: db['{}_{}'.format(blackboard_name, year)] for year in range(2008,2018)}
        for tid, (year, coll) in zip(tag_ids, document_colls.items()):
            obj_id = ObjectId.from_datetime(datetime(year, 1, 1))
            coll.insert({'_id': obj_id, 'T': 'Title {}'.format(tid), 'Tg' : [tid, tid-1], 'FOR' : [11, 12]})

    def generate_standard_blackboard(db, blackboard_name):
        blackboard_name = blackboard_name.upper()

        # Generate tags collection
        tags_coll = db[blackboard_name + TagManager.tag_suffix]
        tag_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        tgs = [{'_id': x, 'Nm': 'Tag_{}'.format(x), 'Ctrl': 0} for x in tag_ids[0:5]]
        tgs.extend([{'_id': x, 'Nm': 'Tag_{}'.format(x), 'DInh': 0} for x in tag_ids[6:10]])
        tgs.extend([{'_id': 11, 'Nm': 'Tag_11', 'Ctrl': 1},{'_id': 12, 'Nm': 'Tag_12', 'Ctrl': 1}]) 
        tags_coll.insert(tgs)

        # Generate counter collection
        counter_coll = db[blackboard_name + CounterManager.counter_suffix]
        counter_coll.insert({"_id" : CounterManager.counter_type, CounterManager.counter_type : CounterManager.counter_type_standard})

        # Generate standard collection
        document_coll = db[blackboard_name]
        for i in range(1, 10):
            document_coll.insert({'_id': i, 'Nm': 'Feed {}'.format(i), 'Tg' : [tag_ids[i]], 'FOR' : [11, 12]})


    def setUp(self):
        random.seed(1234)
        # Settings dont need to be changed when mocking
        self.settings = {'user' : 'test_user', 'password' : 'password', 'dbname' : 'testdb', 
            'dburl' : 'mongodb://localhost:27017'}
        self.api = BlackboardAPI(self.settings, MongoClient=TestBlackboards.mock_client)
        self.bb = self.api.load_blackboard('ARTICLE')

    def tearDown(self):
        del self.api
        del self.bb

    def test_api_drop_blackboard(self):
        with self.assertRaises(ValueError): self.api.drop_blackboard('ARTICLE_TAGS')
        with self.assertRaises(ValueError): self.api.drop_blackboard('article_counter')
        with self.assertRaises(PermissionError): self.api.drop_blackboard('ARTICLE')
        with self.assertRaises(PermissionError): self.api.drop_blackboard('article')

        self.assertEqual(self.api.blackboard_exists('ARTICLE'), True)
        self.assertEqual(self.api.blackboard_exists('ARTICLE2'), True)
        self.assertEqual(self.api.blackboard_exists('FEED'), True)
        self.assertEqual(self.api.blackboard_exists('article'), False)

        # Test admin drop
        settings = {'user' : 'dbadmin', 'password' : 'password', 'dbname' : 'testdb', 'dburl' : 'mongodb://localhost:27017'}
        self.api = BlackboardAPI(settings, MongoClient=TestBlackboards.mock_client)
        
        self.api.drop_blackboard('ARTICLE')
        self.assertEqual(self.api.blackboard_exists('ARTICLE'), False)
        self.assertEqual(self.api.blackboard_exists('ARTICLE2'), True)
        self.api.drop_blackboard('ARTICLE2')
        self.assertEqual(self.api.blackboard_exists('ARTICLE2'), False)
        self.api.drop_blackboard('FEED')
        self.assertEqual(self.api.blackboard_exists('FEED'), False)

        # Test settings validation
        settings = {'user' : 'dbadmin', 'dbname' : 'testdb', 'dburl' : 'mongodb://localhost:27017'}
        with self.assertRaises(ValueError): self.api = BlackboardAPI(settings, MongoClient=TestBlackboards.mock_client)
        settings = {'user' : 'dbadmin', 'dog' : 'dog', 'dbname' : 'testdb', 'dburl' : 'mongodb://localhost:27017'}
        with self.assertRaises(ValueError): self.api = BlackboardAPI(settings, MongoClient=TestBlackboards.mock_client)

    def test_api_load_blackboard(self):
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

    def test_bb_get_tag(self):
        # Bad input
        self.assertEqual(self.bb.get_tag(55), None)
        self.assertEqual(self.bb.get_tag('3'), None)
        self.assertEqual(self.bb.get_tag(tag_name=3333), None)
        self.assertEqual(self.bb.get_tag(tag_name=3), None)

        # Retrieval
        self.assertEqual(self.bb.get_tag(1), {'Ctrl': 0, '_id': 1, 'Nm': 'Tag_1'})
        self.assertEqual(self.bb.get_tag(tag_id=2), {'Ctrl': 0, '_id': 2, 'Nm': 'Tag_2'})
        self.assertEqual(self.bb.get_tag(tag_name='Tag_3'), {'Ctrl': 0, '_id': 3, 'Nm': 'Tag_3'})
        self.assertEqual(self.bb.get_tag(tag_id=12), {'Ctrl': 1, 'Nm': 'POST>Tag_12', '_id': 12})
        

    def test_bb_tag_has_property(self):
        # Bad input for control
        self.assertEqual(self.bb.is_control_tag(55), False)
        self.assertEqual(self.bb.is_control_tag('3'), False)
        
        # Bad input for inheritance
        self.assertEqual(self.bb.is_inheritable_tag(55), False)
        self.assertEqual(self.bb.is_inheritable_tag('3'), False)

        # Checking for control
        self.assertEqual(self.bb.is_control_tag(tag_id=11), True)
        self.assertEqual(self.bb.is_control_tag(tag_name='FOR>Tag_11'), True)
        self.assertEqual(self.bb.is_control_tag(tag_id=4), False)
        self.assertEqual(self.bb.is_control_tag(tag_name='Tag_4'), False)

        # Checking for inheritance (should add True test)
        self.assertEqual(self.bb.is_inheritable_tag(tag_id=11), False)
        self.assertEqual(self.bb.is_inheritable_tag(tag_id=4), False)
        self.assertEqual(self.bb.is_inheritable_tag(tag_id=4), False)
        self.assertEqual(self.bb.is_inheritable_tag(tag_name='Tag_4'), False)

    def test_bb_count(self):
        self.assertEqual(self.bb.count(), 10)
        self.assertEqual(self.bb.count(query={'T' : 'Title 3'}), 1)
        self.assertEqual(self.bb.count(max_date=['02-01-2016'], tags = ['FOR>Tag_11', 12]), 9)

    def test_bb_find(self):
        self.assertEqual(len([x for x in self.bb.find(tags = [3])]), 2)
        self.assertEqual(len([x for x in self.bb.find(tags = ['FOR>Tag_11', 12])]), 10)
        self.assertEqual(len([x for x in self.bb.find(min_date=['01-01-2016'], tags = ['FOR>Tag_11', 12])]), 2)
        self.assertEqual(len([x for x in self.bb.find(max_date=['02-01-2016'], tags = ['FOR>Tag_11', 12])]), 9)
        self.assertEqual(len([x for x in self.bb.find(tags = ['FOR>Tag_11', 5])]), 2)
        self.assertEqual(len([x for x in self.bb.find(max_date=['21-01-2015'], min_date=['21-01-2007'], tags = ['POST>Tag_12', 3], fields=['T','Tg'], without_fields=['D'])]), 2)
        self.assertEqual([x for x in self.bb.find(query={'T' : 'Title 3'})][0]['T'], 'Title 3')

        with self.assertRaises(ValueError): self.bb.find(tags = [1, 13])
        with self.assertRaises(ValueError): self.bb.find(tags = ['Tag_4', 13])
        
    def test_bb_get_date(self):
        self.assertEqual(str(self.bb.get_date({DocumentManager.doc_id: ObjectId.from_datetime(dtparser.parse('21-10-2017'))})), '2017-10-21 00:00:00+00:00')
        with self.assertRaises(ValueError): self.bb.get_date({'different_id': ObjectId.from_datetime(dtparser.parse('21-10-2017'))})
        with self.assertRaises(ValueError): self.bb.get_date({DocumentManager.doc_id: 1})

    def test_get_extremal_date(self):
        self.assertEqual(str(self.bb.get_earliest_date()), '2008-01-01 00:00:00+00:00')
        self.assertEqual(str(self.bb.get_latest_date()), '2017-01-01 00:00:00+00:00')

    def test_insert(self):
        with self.assertRaises(NotImplementedError): self.bb.insert({DocumentManager.doc_id : ObjectId.from_datetime(dtparser.parse('21-10-2017'))})

    def test_update(self):
        with self.assertRaises(NotImplementedError): self.bb.update(ObjectId.from_datetime(dtparser.parse('21-10-2017')), {})

    def test_delete(self):
        with self.assertRaises(PermissionError): self.bb.delete(ObjectId.from_datetime(dtparser.parse('21-10-2017')))
        # Add test as admin

    def test_insert_tag(self):
        # ValueErrors
        with self.assertRaises(NotImplementedError): self.bb.insert_tag('Tag_Neg')
        with self.assertRaises(NotImplementedError): self.bb.insert_tag('Tag_0')
        with self.assertRaises(NotImplementedError): self.bb.insert_tag('Tag_1')
        with self.assertRaises(NotImplementedError): self.bb.insert_tag('FOR>Tag_15', False)
        with self.assertRaises(NotImplementedError): self.bb.insert_tag('POST>Tag_16', False)

        # Success
        with self.assertRaises(NotImplementedError): self.bb.insert_tag(13, 'Tag_13')
        with self.assertRaises(NotImplementedError): self.bb.insert_tag(14, 'FOR>Tag_14', True)
        
    def test_update_tag(self):
        with self.assertRaises(NotImplementedError): self.bb.update_tag(1, {TagManager.tag_control : 1})

    def test_delete_tag(self):
        with self.assertRaises(PermissionError): self.bb.delete_tag(1)
        # Add test as admin

    def test_add_tag(self):
        # Need to complete
        doc = [x for x in self.bb.find(query={'T' : 'Title 3'})][0]
        result = self.bb.add_tag(doc[DocumentManager.doc_id], 1)
        self.assertEqual(result['err'], None)

    def test_remove_tag(self):
        # Need to complete
        doc = [x for x in self.bb.find(query={'T' : 'Title 3'})][0]
        result = self.bb.remove_tag(doc[DocumentManager.doc_id], 3)
        self.assertEqual(result['err'], None)

if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestBlackboards)
    unittest.TextTestRunner().run(suite)
