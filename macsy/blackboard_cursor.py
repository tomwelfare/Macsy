import pymongo
import mongomock
import sys


__all__ = ['BlackboardCursor']

class BlackboardCursor:

	def __init__(self, cursors, max_docs=0):
		self.__cursors = [x for x in cursors if x.count() > 0]
		self.__current = 0
		self.__index = 0

	def __iter__(self):
		return self

	def __next__(self):
		while self.__current < len(self.__cursors):
			if self.__index < self.__cursors[self.__current].count():
				doc = self.__cursors[self.__current][self.__index]
				self.__index += 1
				return doc
			else:
				self.__index = 0
				self.__current += 1
		raise StopIteration()
