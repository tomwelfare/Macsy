from macsy import *

#settings = {'user' : 'test_user', 'password' : 'password', 'dbname' : 'testdb', 'dburl' : 'mongodb://localhost:27017/'}
#api = BlackboardAPI(settings)
#bb = api.load_blackboard('ARTICLE') # date-based

# utility
#doc_id = "5b3d51700000000000000000" # or ObjectId("5b3d51700000000000000000")?
#bb.get_date(doc_id) # ObjectId.generation_time

# check indexes
#bb.check_indexes()
#fields = ['Tg','FOR']
#bb.ensure_index(fields)

# count methods
#num_docs = bb.count()

# tag retrieval methods
#tag = bb.get_tag(tag_id=5) # returns e.g. {'id' : 5, 'name' : 'FOR>Scraper'}
#tag = bb.get_tag(tag_name='FOR>Scraper') # returns e.g. {'id' : 5, 'name' : 'FOR>Scraper'}

# document retrieval methods
#docs  = bb.find() # returns all docs

#doc_id = "5b3d51700000000000000000" # or ObjectId("5b3d51700000000000000000")?
#doc = bb.find(doc_id=doc_id)

#query_tags = [1, 2, 3]
#docs = bb.find(tags=query_tags)

#query_tags = [1, 2, 3]
#without_tags = [4, 5]
#with_fields = ['T','C']
#without_fields = ['XML']
#docs = bb.find(tags = query_tags, without_tags = without_tags, fields = with_fields, without_fields = without_fields)

# edit methods
#doc = {'T' : 'title blah blah', 'D': 'Description blah', 'C', 'Content etc', 'Fds' : [1, 2, 3], 'Tg' : [1, 2, 3], 'FOR' : [5, 6, 7]}
#bb.insert(doc)
#bb.remove(doc) # admin_mode only

#tag_name = 'new_tag'
#tag = bb.insert_tag(tag_name) # returns e.g. {'id' : 6, 'name' : 'new_tag'}


#tag_id = 1 # or set of integers
#doc_id = "5b3d51700000000000000000" # or ObjectId("5b3d51700000000000000000")?
#bb.add_tags(doc_id,tag_id)

#tag_id = 1 # or set of integers
#doc_id = "5b3d51700000000000000000" # or ObjectId("5b3d51700000000000000000")?
#bb.remove_tags(doc_id,tag_id)

#field = 'XML' # or set of strings
#doc_id = "5b3d51700000000000000000" # or ObjectId("5b3d51700000000000000000")?
#bb.remove_fields(doc_id, field)

#field = 'XML' # or set of strings
#value = 'Dog' # or list of anything
#doc_id = "5b3d51700000000000000000" # or ObjectId("5b3d51700000000000000000")?
#bb.add_fields(doc_id, field, value)

# date-based only methods
#earliest = bb.get_earliest_date() # returns date
#latest = bb.get_latest_date() # returns date