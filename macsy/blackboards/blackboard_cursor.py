class BlackboardCursor:

    def __init__(self, cursors):
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

    def __len__(self):
        return sum([x.count() for x in self.__cursors])
