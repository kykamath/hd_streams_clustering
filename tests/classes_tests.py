'''
Created on Jun 22, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
import unittest
from classes import Stream

class StreamTests(unittest.TestCase):
    def test_initialization(self):
        stream = Stream()


if __name__ == '__main__':
    unittest.main()