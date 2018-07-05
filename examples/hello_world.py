import sys
sys.path.insert(0, '../macsy')
from macsy.blackboard_api import BlackboardAPI


settings = {'user' : 'test_user', 'password' : 'password', 'dbname' : 'testdb', 'dburl' : 'mongodb://localhost:27017/'}
api = BlackboardAPI(settings)
bb = api.load_blackboard('ARTICLE') # date-based

for doc in bb.find():
	print(doc)