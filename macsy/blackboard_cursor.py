import pymongo

__all__ = ['BlackboardCursor']

class BlackboardCursor:

	def __init__(self, cursors):
		self.__cursors = []
		if isinstance(cursors, type(pymongo.cursor)):
			self.__cursors.append(cursors)
		else:
			self.__cursors.extend(cursors)
		self.__current = 0
		self.__index = 0
		self.__current_size = self.__cursors[self.__current].count()

	def __iter__(self):
		return self

	def __next__(self):
		while self.__current < len(self.__cursors):
			if self.__cursors[self.__current].alive:
				return self.__cursors[self.__current].__next__()
			self.__current += 1
		raise StopIteration()

	def count(self):	
		return sum(cursor.count() for cursor in self.__cursors)
