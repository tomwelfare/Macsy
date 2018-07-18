import sys
import os.path
home = '/'.join(os.path.abspath(__file__).split('/')[0:-2])
sys.path.insert(0, home)
import unittest
from test.test_blackboards import TestBlackboards
from test.test_date_based_blackboards import TestDateBasedBlackboards
from test.test_blackboard_api import TestBlackboardAPI
from test.test_managers import TestManagers

if __name__ == '__main__':
    test_classes = [TestBlackboardAPI, TestBlackboards, TestDateBasedBlackboards, TestManagers]
    loader = unittest.TestLoader()
    suites_list = []
    for test_class in test_classes:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
    suites = unittest.TestSuite(suites_list)
    unittest.TextTestRunner().run(suites)