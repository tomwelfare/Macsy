class BlackboardCursor:

    def __init__(self, cursors_and_max_docs):
        cursors, max_docs = cursors_and_max_docs
        self.__cursors = [x for x in cursors if x.count() > 0] if(isinstance(cursors,list)) else [cursors]
        self.__current = 0
        self.__index = 0
        self.__retrieved = 0
        self.__max = max_docs


    def __iter__(self):
        return self

    def __next__(self):
        while self.__current < len(self.__cursors):
            if self.__index < self.__cursors[self.__current].count():
                self._check_max_retrieved()
                doc = self.__cursors[self.__current][self.__index]
                self.__index += 1
                self.__retrieved += 1        
                return doc
            else:
                self.__index = 0
                self.__current += 1
        raise StopIteration()

    def __len__(self):
        return sum([x.count() for x in self.__cursors])

    def _check_max_retrieved(self):
        if self.__retrieved >= self.__max > 0:
            raise StopIteration()
