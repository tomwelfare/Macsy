import sys
import os.path
import mongomock 
home = '/'.join(os.path.abspath(__file__).split('/')[0:-2])
sys.path.insert(0, home)
from datetime import datetime
from bson.objectid import ObjectId
from macsy.blackboards import blackboard_api, blackboard, date_based_blackboard
from macsy.blackboards.managers import tag_manager, document_manager, counter_manager

BlackboardAPI = blackboard_api.BlackboardAPI
Blackboard = blackboard.Blackboard
DateBasedBlackboard = date_based_blackboard.DateBasedBlackboard
TagManager = tag_manager.TagManager
DocumentManager = document_manager.DocumentManager
CounterManager = counter_manager.CounterManager

def mock_client(*args, **kwargs):
    client = mongomock.MongoClient(*args, **kwargs)
    db = client['testdb']

    generate_date_based_blackboard(db,'ARTICLE')
    generate_date_based_blackboard(db,'ARTICLE2')
    generate_standard_blackboard(db,'FEED')

    return client

def settings():
    return {'user' : 'test_user', 'password' : 'password', 'dbname' : 'testdb', 'dburl' : 'mongodb://localhost:27017'}

def admin_settings():
    return {'user' : 'dbadmin', 'password' : 'password', 'dbname' : 'testdb', 'dburl' : 'mongodb://localhost:27017'}

def generate_date_based_blackboard(db, blackboard_name):
    blackboard_name = blackboard_name.upper()

    # Generate tags collection
    tags_coll = db[blackboard_name + TagManager.tag_suffix]
    tag_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tgs = [{'_id': x, 'Nm': 'Tag_{}'.format(x), 'Ctrl': 0} for x in tag_ids]
    tgs.append({'_id': 11, 'Nm': 'FOR>Tag_11', 'Ctrl': 1})
    tgs.append({'_id': 12, 'Nm': 'POST>Tag_12', 'Ctrl': 1})    
    tags_coll.insert(tgs)

    # Generate counter collection
    counter_coll = db[blackboard_name + CounterManager.counter_suffix]
    counter_coll.insert({"_id" : CounterManager.counter_type, CounterManager.counter_type : CounterManager.counter_type_date_based})
    counter_coll.insert({"_id" : CounterManager.counter_next, CounterManager.counter_tag : tags_coll.count() + 1})

    # Generate date_based collections
    document_colls = {year: db['{}_{}'.format(blackboard_name, year)] for year in range(2009,2019)}
    for tid, (year, coll) in zip(tag_ids, document_colls.items()):
        obj_id = ObjectId.from_datetime(datetime(year, 1, 1))
        coll.insert({'_id': obj_id, 'T': 'Title {}'.format(tid), 'Tg' : [tid, tid-1], 'FOR' : [11, 12]})

def generate_standard_blackboard(db, blackboard_name):
    blackboard_name = blackboard_name.upper()

    # Generate tags collection
    tags_coll = db[blackboard_name + TagManager.tag_suffix]
    tag_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tgs = [{'_id': x, 'Nm': 'Tag_{}'.format(x), 'Ctrl': 0} for x in tag_ids[0:5]]
    tgs.extend([{'_id': x, 'Nm': 'Tag_{}'.format(x), 'DInh': 0} for x in tag_ids[5:10]])
    tgs.extend([{'_id': 11, 'Nm': 'FOR>Tag_11', 'Ctrl': 1},{'_id': 12, 'Nm': 'POST>Tag_12', 'Ctrl': 1}])
    tags_coll.insert(tgs)

    # Generate standard collection
    document_coll = db[blackboard_name]
    for i in range(1, 11):
        doc = {'_id': i, 'Nm': 'Feed {}'.format(i), 'Tg' : [tag_ids[i-1]], 'FOR' : [11, 12]}
        if i == 5:
            doc['Single'] = True
        document_coll.insert(doc)

    # Generate counter collection
    counter_coll = db[blackboard_name + CounterManager.counter_suffix]
    counter_coll.insert({"_id" : CounterManager.counter_type, CounterManager.counter_type : CounterManager.counter_type_standard})
    counter_coll.insert({"_id" : CounterManager.counter_next, CounterManager.counter_tag : tags_coll.count() + 1, CounterManager.counter_doc : document_coll.count()+1})
