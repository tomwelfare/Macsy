class BlackboardCursor:

    def __init__(self, cursors_and_max_docs):
        cursors, max_docs = cursors_and_max_docs
        self.__cursors = [x for x in cursors if x.count() > 0] if(isinstance(cursors,list)) else [cursors]
        self.__current = 0
        self.__retrieved = 0
        self.__max_docs = max_docs

    def __iter__(self):
        return self

    def __next__(self):
        while self.__current < len(self.__cursors):
            self._retrieved_max()
            try:
                return next(self.__cursors[self.__current])
            except StopIteration:
                self.__retrieved -= 1
                self.__current += 1
            finally:
                self.__retrieved += 1
        raise StopIteration()

    def __len__(self):
        count = sum([x.count() for x in self.__cursors]) 
        return count if self.__max_docs == 0 or count < self.__max_docs else self.__max_docs

    def _retrieved_max(self):
        if self.__max_docs > 0 and self.__retrieved >= self.__max_docs:
            raise StopIteration()
