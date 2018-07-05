import pymongo

__all__ = ['BlackboardCursor']

class BlackboardCursor:

	__cursors = []
	__current = 0

	def __init__(self, cursors):
		if isinstance(cursors, pymongo.cursor):
			cursors = [cursors]
		self.__cursors = cursors

	def __iter__(self):
		return self

	def __next__(self):
		if self.__current < len(self.__cursors):
			doc = self.__cursors[self.__current].next()
			if doc is not None:
				return doc
			else:
				self.__current += 1
				self.__next__()
		else:
			return None
