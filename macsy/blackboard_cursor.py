import pymongo

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
		while self.__current < len(self.__cursors):
			if self.__cursors[self.__current].alive:
				return self.__cursors[self.__current].__next__()
			self.__current += 1
		raise StopIteration()

	def count(self):
		total = 0
		for i in range(0,len(self.__cursors)):
			total += self.__cursors[i].count()
		return total
