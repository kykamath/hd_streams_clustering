'''
Created on Jun 27, 2011

@author: kykamath
'''
import unittest
from datetime import datetime, timedelta
from hd_streams_clustering import DataStreamMethods

test_time = datetime.now()

class DataStreamMethodsTests(unittest.TestCase):
    def test_messageInOrder(self):
        messageSequence = [test_time+timedelta(minutes=i) for i in range(5)]
        messageSequence[2]=messageSequence[4]=test_time
        self.assertEqual([True, True, False, True, False], [DataStreamMethods.messageInOrder(t) for t in messageSequence])
        
if __name__ == '__main__':
    unittest.main()