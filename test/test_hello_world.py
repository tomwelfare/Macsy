from macsy.blackboard_api import BlackboardAPI
from macsy.blackboard import Blackboard
import mongomock 
import pymongo
import itertools
from datetime import datetime
from dateutil import parser as dtparser
from bson.objectid import ObjectId
import random

random.seed(1234)

# Settings dont need to be changed when mocking
settings = {'user' : 'test_user', 'password' : 'password', 'dbname' : 'testdb', 
		'dburl' : 'mongodb://localhost:27017'}

# Intercept the constructor call to mongoclient so we can capture it. 
# As the blackboard class probably makes calls to mongo we need to 
# define our mocked dbs and collections here. Either prefill collections
# here or insert between blackboard calls to check how it handles changes
def client_constructor(*args, **kwargs):
	global client, db, tags_coll, counter_coll, document_colls, tag_ids
	client = mongomock.MongoClient(*args, **kwargs)

	db = client[settings['dbname']]

	tags_coll = db['ARTICLE_TAGS']
	tag_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
	tgs = [{'_id': x, 'Nm': 'Tag_{}'.format(x), 'Ctrl': 0} for x in tag_ids]
	tgs.extend([{'_id': 11, 'Nm': 'FOR>Tag_11', 'Ctrl': 1},{'_id': 12, 'Nm': 'POST>Tag_12', 'Ctrl': 1}])	
	tags_coll.insert(tgs)

	counter_coll = db['ARTICLE_COUNTER']
	counter_coll.insert({"_id" : Blackboard.counter_type, Blackboard.counter_type : Blackboard.counter_type_date_based})

	document_colls = {year: db['ARTICLE_{}'.format(year)] for year in range(2008,2018)}

	for tid, (year, coll) in zip(tag_ids, document_colls.items()):
		obj_id = ObjectId.from_datetime(datetime(year, 1, 1))
		coll.insert({'_id': obj_id, 'T': 'Title {}'.format(tid), 'Tg' : [tid, tid-1], 'FOR' : [11, 12]})

	return client

def test_bb_get_tag(bb):
	# Bad input
	tag = bb.get_tag(55)
	assert tag == None, 'bb.get_tag(tag_id=fake_id) failed: {}'.format(tag)
	tag = bb.get_tag('3')
	assert tag == None, 'bb.get_tag(tag_id=str_id) failed: {}'.format(tag)
	tag = bb.get_tag(tag_name=3333)
	assert tag == None, 'bb.get_tag(tag_name=int) failed: {}'.format(tag)
	tag = bb.get_tag(tag_name=3)
	assert tag == None, 'bb.get_tag(tag_name=int) failed: {}'.format(tag)
	boolean = bb.is_control_tag(55)
	assert boolean == False, 'bb.is_control_tag(tag_id=id) returned the wrong value: {}'.format(boolean)
	boolean = bb.is_control_tag('3')
	assert boolean == False, 'bb.is_control_tag(tag_id=id) returned the wrong value: {}'.format(boolean)	

	# Retrieval
	tag = bb.get_tag(tag_ids[0])
	assert tag == {'Ctrl': 0, '_id': 1, 'Nm': 'Tag_1'}, 'bb.get_tag(id) found the wrong tag: {}'.format(tag)
	tag = bb.get_tag(tag_id=2)
	assert tag == {'Ctrl': 0, '_id': 2, 'Nm': 'Tag_2'}, 'bb.get_tag(tag_id=id) found the wrong tag: {}'.format(tag)
	tag = bb.get_tag(tag_name='Tag_3')
	assert tag == {'Ctrl': 0, '_id': 3, 'Nm': 'Tag_3'}, 'bb.get_tag(tag_name=name) found the wrong tag: {}'.format(tag)
	tag = bb.get_tag(tag_id=12)
	assert tag == {'Ctrl': 1, 'Nm': 'POST>Tag_12', '_id': 12}, 'bb.get_tag(tag_id=id) failed for a control tag: {}'.format(tag)
	boolean = bb.is_control_tag(tag_id=11)
	assert boolean == True, 'bb.is_control_tag(tag_id=id) returned the wrong value: {}'.format(boolean)
	boolean = bb.is_control_tag(tag_id=4)
	assert boolean == False, 'bb.is_control_tag(tag_id=id) returned the wrong value: {}'.format(boolean)	

def test_bb_count(bb):
	count = bb.count()
	assert count == 10, 'bb.count() had incorrect count: {}'.format(count)
	count = bb.count(query={'T' : 'Title 3'})
	assert count == 1, 'bb.count() had incorrect count: {}'.format(count)

def test_bb_find(bb):
	docs = [x for x in bb.find(tags = [3])]
	assert len(docs) == 2,	'bb.find(tags=tags) found the wrong documents: {}'.format(docs)
	docs = [x for x in bb.find(tags = ['FOR>Tag_11', 12])]
	assert len(docs) == 10,	'bb.find(tags=mixed_tags) found the wrong documents: {}'.format(docs)
	docs = [x for x in bb.find(min_date=['01-01-2016'], tags = ['FOR>Tag_11', 12])]
	assert len(docs) == 2,	'bb.find(min_date=date, tags=mixed_tags, without_tags=missing_tags) found the wrong documents: {}'.format(docs)	
	docs = [x for x in bb.find(max_date=['02-01-2016'], tags = ['FOR>Tag_11', 12])]
	assert len(docs) == 9,	'bb.find(max_date=date, tags=mixed_tags) found the wrong documents: {}'.format(docs)
	docs = [x for x in bb.find(tags = ['FOR>Tag_11', 5])]
	assert len(docs) == 2,	'bb.find(tags=mixed_ctrl_tags) found the wrong documents: {}'.format(docs)
	docs = [x for x in bb.find(max_date=['21-01-2015'], min_date=['21-01-2007'], tags = ['POST>Tag_12', 3], fields=['T','Tg'], without_fields=['D'])]
	assert len(docs) == 2,	'bb.find(max_date=date, min_date=date, tags=mixed_ctrl_tags, fields=fields, without_fields = without_fields) found the wrong documents: {}'.format(docs)
	doc = [x for x in bb.find(query={'T' : 'Title 3'})][0]
	assert doc['T'] == 'Title 3', 'bb.find(query=query) found the wrong document: {}'.format(doc)

def test_bb_get_date(bb):
	date = bb.get_date({'_id': ObjectId.from_datetime(dtparser.parse('21-10-2017'))})
	assert str(date) == '2017-10-21 00:00:00+00:00', 'bb.get_date(doc) found the wrong date: {}'.format(date)


# If we dont supply a mock MongoDB constructor, it will actually open a connection.
# Need to be careful with this when using state changing methods if we do end up
# testing without mock.
api = BlackboardAPI(settings, MongoClient=client_constructor)
bb = api.load_blackboard('ARTICLE') # date-based

test_bb_get_tag(bb)
test_bb_count(bb)
test_bb_find(bb)
test_bb_get_date(bb)
