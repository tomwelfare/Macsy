import sys
sys.path.insert(0, '../macsy')
from macsy.blackboard_api import BlackboardAPI

settings = {'user' : 'test_user', 'password' : 'password', 'dbname' : 'testdb', 'dburl' : 'mongodb://localhost:27017'}
api = BlackboardAPI(settings)
bb = api.load_blackboard('ARTICLE', date_based=True) # date-based

print('Total docs: %d' % bb.count())
#print('Docs with tags: %d' % bb.find().count())

for doc in bb.find(min_date=2014, max_date=ObjectId("gwrgwrhgirwehiwruoghwgo"), with_tags=["FOR>SCRAPER",2,3], fields=['XML','DOG'], without_fields=['BAD','WORSE']):
	print(doc)