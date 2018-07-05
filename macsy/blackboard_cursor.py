import pymongo
import sys

__all__ = ['BlackboardCursor']

class BlackboardCursor:

	__cursors = []
	__current = 0
	__current_size = 1
	__index = 0

	def __init__(self, cursors):
		if isinstance(cursors, type(pymongo.cursor)):
			cursors = [cursors]
		self.__cursors = cursors
		self.__current_size = self.__cursors[self.__current].count()

	def __iter__(self):
		return self

	def __next__(self):
		if self.__index < self.__current_size and self.__current < len(self.__cursors):		
			print('%d of %d, in cursor %d (total cursors: %d)' % (self.__index, self.__current_size, self.__current, len(self.__cursors)))
			doc = self.__cursors[self.__current][self.__index]
			self.__index += 1
			return doc # why is this None when called in hello_world??
		if self.__index == self.__current_size:
			self.__cursors[self.__current].close()
			self.__current += 1
			if self.__current >= len(self.__cursors):
				raise StopIteration()
			self.__index = 0
			self.__current_size = self.__cursors[self.__current].count()
			self.__next__()
		

	def count(self):
		total = 0
		for i in range(0,len(self.__cursors)):
			total += self.__cursors[i].count()
		return total
