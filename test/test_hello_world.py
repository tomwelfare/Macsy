from macsy.blackboard_api import BlackboardAPI
from macsy.blackboard import Blackboard
import mongomock 
import itertools

# Settings dont need to be changed when mocking
settings = {'user' : 'test_user', 'password' : 'password', 'dbname' : 'testdb', 
		'dburl' : 'mongodb://localhost:27017'}

# Intercept the constructor call to mongoclient so we can capture it. 
# As the blackboard class probably makes calls to mongo we need to 
# define our mocked dbs and collections here. Either prefill collections
# here or insert between blackboard calls to check how it handles changes
def client_constructor(*args, **kwargs):
	global client, db, tags_coll, counter_coll, document_colls
	client = mongomock.MongoClient(*args, **kwargs)

	db = client[settings['dbname']]

	# It'll automatically create collections if you insert, but I want
	# to leave this empty for now
	tags_coll = db.create_collection('ARTICLE_TAGS')

	counter_coll = db['ARTICLE_COUNTER']
	counter_coll.insert({'_id': Blackboard.counter_type_date_based})

	document_colls = {year: db.create_collection('ARTICLE_{}'.format(year)) for year in range(2014,2018)}

	# Give them doc_ids
	# NOTE I'm not sure what form they take yet
	# For this test they dont need to be correct
	for i, coll in zip(itertools.count(), document_colls.values()):
		coll.insert({'doc_id':i})

	return client

# If we dont supply a mock MongoDB constructor, it will actually open a connection.
# Need to be careful with this when using state changing methods if we do end up
# testing without mock.
api = BlackboardAPI(settings, MongoClient=client_constructor)
bb = api.load_blackboard('ARTICLE', date_based=True) # date-based

count = bb.count()
assert count == 4, 'Date base blackboard had incorrect count: {}'.format(count)

# TODO Use bb.insert_doc(...)
document_colls[2015].insert({"Hello": "world"})

count = bb.count()
assert count == 5, 'Date base blackboard had incorrect count: {}'.format(count)



