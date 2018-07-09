from macsy.blackboard_api import BlackboardAPI
from macsy.blackboard import Blackboard
import mongomock 
import pymongo
import itertools
from datetime import datetime
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
	tag_ids = random.sample(range(1, 20), 5)
	tgs = [{'_id': x, 'Nm': 'Tag_{}'.format(x), 'Ctrl': 0} for x in tag_ids]
	tgs.extend([{'_id': 21, 'Nm': 'FOR>Tag_21', 'Ctrl': 1},{'_id': 22, 'Nm': 'POST>Tag_22', 'Ctrl': 1}])	
	tags_coll.insert(tgs)

	counter_coll = db['ARTICLE_COUNTER']
	counter_coll.insert({'_id': Blackboard.counter_type_date_based})

	document_colls = {year: db['ARTICLE_{}'.format(year)] for year in range(2014,2018)}

	for tid, (year, coll) in zip(tag_ids, document_colls.items()):
		obj_id = ObjectId.from_datetime(datetime(year, 1, 1))
		coll.insert({'_id': obj_id, 'T': 'Title {}'.format(tid), 'Tg' : [tid], 'FOR' : [21, 22]})

	return client

# If we dont supply a mock MongoDB constructor, it will actually open a connection.
# Need to be careful with this when using state changing methods if we do end up
# testing without mock.
api = BlackboardAPI(settings, MongoClient=client_constructor)
bb = api.load_blackboard('ARTICLE', date_based=True) # date-based

count = bb.count()
assert count == 4, 'Date-based blackboard had incorrect count: {}'.format(count)

# Test bb.get_tag()
tag = bb.get_tag(tag_ids[0])
assert tag == {'Ctrl': 0, '_id': 15, 'Nm': 'Tag_15'}, 'bb.get_tag(id) found the wrong tag: {}'.format(tag)
tag = bb.get_tag(tag_id=19)
assert tag == {'Ctrl': 0, '_id': 19, 'Nm': 'Tag_19'}, 'bb.get_tag(tag_id=id) found the wrong tag: {}'.format(tag)
tag = bb.get_tag(tag_name='Tag_3')
assert tag == {'Ctrl': 0, '_id': 3, 'Nm': 'Tag_3'}, 'bb.get_tag(tag_name=name) found the wrong tag: {}'.format(tag)
tag = bb.get_tag(tag_id=22)
assert tag == {'Ctrl': 1, 'Nm': 'POST>Tag_22', '_id': 22}, 'bb.get_tag(tag_id=id) failed for a control tag: {}'.format(tag)
boolean = bb.is_control_tag(tag_id=22)
assert boolean == True, 'bb.is_control_tag(tag_id=id) returned the wrong value: {}'.format(boolean)
boolean = bb.is_control_tag(tag_id=15)
assert boolean == False, 'bb.is_control_tag(tag_id=id) returned the wrong value: {}'.format(boolean)

##TODO: Test bb.find()
docs = [x for x in bb.find(tags = [3])]
assert len(docs) == 1,	'bb.find(tags=tags) found the wrong documents: {}'.format(docs)

docs = [x for x in bb.find(tags = ['FOR>Tag_21', 22])]
assert len(docs) == 4,	'bb.find(tags=mixed_tags) found the wrong documents: {}'.format(docs)

docs = [x for x in bb.find(min_date='01-01-2016', tags = ['FOR>Tag_21', 22])]
assert len(docs) == 2,	'bb.find(min_date=date, tags=mixed_tags) found the wrong documents: {}'.format(docs)

docs = [x for x in bb.find(max_date='02-01-2016', tags = ['FOR>Tag_21', 22])]
assert len(docs) == 3,	'bb.find(max_date=date, tags=mixed_tags) found the wrong documents: {}'.format(docs)

docs = [x for x in bb.find(tags = ['FOR>Tag_21', 15])]
assert len(docs) == 1,	'bb.find(tags=mixed_ctrl_tags) found the wrong documents: {}'.format(docs)

docs = [x for x in bb.find(max_date='21-01-2015', tags = ['POST>Tag_22', 3], fields=['T','Tg'], without_fields=['D'])]
assert len(docs) == 2,	'bb.find(max_date=date, tags=mixed_ctrl_tags, fields=fields, without_fields = without_fields) found the wrong documents: {}'.format(docs)

#docs = [x for x in bb.find(min_date='01-01-2014', max_date='01-01-2018', max_docs = 1)]
#assert len(docs) == 1, 'bb.find(min_date=date, max_date=date, max_docs=1) found the wrong document: {}'.format(docs)

doc = [x for x in bb.find(query={'T' : 'Title 3'})][0]
assert doc == {'T': 'Title 3', '_id': ObjectId('54a48e000000000000000000'), 'FOR': [21, 22], 'Tg': [3]}, 'bb.find(query=query) found the wrong document: {}'.format(doc)

# Test bb.get_date()
date = bb.get_date(doc)
assert str(date) == '2015-01-01 00:00:00+00:00', 'bb.get_date(doc) found the wrong date: {}'.format(date)