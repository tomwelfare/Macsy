class BlackboardCursor:

    def __init__(self, cursors_and_max_docs):
        cursors, max_docs = cursors_and_max_docs
        self.__cursors = [x for x in cursors if x.count() > 0] if(isinstance(cursors,list)) else [cursors]
        self.__current = 0


    def __iter__(self):
        return self

    def __next__(self):
        while self.__current < len(self.__cursors):
            try:
                return next(self.__cursors[self.__current])
            except StopIteration:
                self.__current += 1
        raise StopIteration()

    def __len__(self):
        return sum([x.count() for x in self.__cursors])
